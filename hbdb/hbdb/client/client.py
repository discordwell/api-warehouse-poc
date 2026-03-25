import socket
import json
import threading
from typing import Dict, Any, Optional
from hbdb.core.topology import ClusterTopology

class HBDBClient:
    """Represents a connection to a SINGLE node."""
    def __init__(self, host="localhost", port=9000):
        self.host = host
        self.port = port
        self.sock = None
        self._lock = threading.RLock()
        # Connect lazily? No, connect now.
        # But if sharding, we have many. connect lazy is better.
        # self._connect() 

    def _connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(2.0)
            self.sock.connect((self.host, self.port))
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._file = self.sock.makefile('rb')
            # print(f"[HBDBClient] Connected to {self.host}:{self.port}")
        except Exception as e:
            self.sock = None
            # print(f"[HBDBClient] Error connecting to {self.host}:{self.port}: {e}")
            raise e

    def send(self, data: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            if not self.sock:
                self._connect()
            
            try:
                msg = json.dumps(data).encode() + b"\n"
                self.sock.sendall(msg)
                
                line = self._file.readline()
                if not line:
                    raise ConnectionResetError("Server closed connection")
                    
                resp = json.loads(line)
                if resp.get("status") == "error":
                    raise RuntimeError(resp.get("msg", "Unknown Error"))
                return resp
            except Exception as e:
                # Close and retry/raise
                if self.sock: self.sock.close()
                self.sock = None
                raise e

class ClusterClient:
    """Manages connections to Coordinator and Storage Nodes."""
    def __init__(self, coordinator_addr: str, storage_addrs: list, rf: int = 1):
        h, p = coordinator_addr.split(":")
        self.coordinator = HBDBClient(h, int(p))
        
        self.storage_nodes = []
        parsed_storage = []
        for s in storage_addrs:
            h, p = s.split(":")
            client = HBDBClient(h, int(p))
            self.storage_nodes.append(client)
            parsed_storage.append((h, int(p)))
            
        self.topology = ClusterTopology(parsed_storage, replication_factor=rf)
        
    def get_replica_clients(self, key: str) -> list[HBDBClient]:
        """Returns list of clients for replicas holding the key."""
        nodes = self.topology.get_nodes_for_key(key) # [(h,p), (h,p)]
        
        clients = []
        for host, port in nodes:
            # Find matching client
            for c in self.storage_nodes:
                if c.host == host and c.port == port:
                    clients.append(c)
                    break
        return clients

    def get_storage_client(self, key: str) -> HBDBClient:
        # Backward compatibility / Primary
        return self.get_replica_clients(key)[0]

    def send_to_coordinator(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self.coordinator.send(data)


class NetworkBackend:
    def __init__(self, cluster: ClusterClient):
        self.cluster = cluster
        
    def read(self, key: str, read_ts: int) -> Optional[Any]:
        # High Availability Read: Try Replicas in random order
        import random
        clients = self.cluster.get_replica_clients(key)
        # Shuffle for load balancing
        # Create a copy to shuffle
        candidates = list(clients)
        random.shuffle(candidates)
        
        last_err = None
        for client in candidates:
            try:
                req = {"cmd": "get", "key": key, "ts": read_ts}
                resp = client.send(req)
                return resp.get("value")
            except Exception as e:
                last_err = e
                continue
                
        # If all failed
        if last_err: raise last_err
        return None

    def write(self, key: str, value: Any, commit_ts: int):
        pass # Client does not write directly

    def scan(self, start: str, end: str, read_ts: int):
        # Scan is hard in sharded. Keys might be anywhere.
        # Simple Scatter-Gather.
        results = []
        for client in self.cluster.storage_nodes:
            req = {"cmd": "scan", "start": start, "end": end, "ts": read_ts}
            try:
                resp = client.send(req)
                if resp.get("data"):
                    results.extend(resp["data"])
            except:
                pass
        # Sort/Dedupe?
        # Typically we just merge. Backend native scan returns specific keys.
        results.sort(key=lambda x: x[0])
        return results

class NetworkResolver:
    def __init__(self, cluster: ClusterClient):
        self.cluster = cluster
