from typing import Dict, Any, List, Optional
from hbdb.client.client import ClusterClient, NetworkBackend

class NetworkTransaction:
    def __init__(self, cluster: ClusterClient):
        self.cluster = cluster
        self.backend = NetworkBackend(cluster) # For reads
        
        # Get Read Timestamp from Oracle (Coordinator)
        resp = self.cluster.send_to_coordinator({"cmd": "get_time"})
        self.read_ts = resp.get("ts", 0)
        
        self.writes: Dict[str, Any] = {}
        self.read_keys: List[str] = []
        self.read_ranges: List[List[str]] = []
        
    def get(self, key: str) -> Optional[Any]:
        # Check local writes
        if key in self.writes:
            return self.writes[key]
        
        # Determine Read TS if not set?
        # Assuming Snapshot Isolation, we need a consistent Read TS.
        # Let's mock it or ask server.
        
        self.read_keys.append(key)
        val = self.backend.read(key, self.read_ts)
        return val

    def set(self, key: str, value: Any):
        self.writes[key] = value

    def scan(self, start: str, end: str):
        self.read_ranges.append([start, end])
        return self.backend.scan(start, end, self.read_ts)

    def commit(self) -> bool:
        if not self.writes:
            return True
            
        # Send everything to server
        req = {
            "cmd": "commit",
            "read_ts": self.read_ts,
            "read_keys": self.read_keys,
            "read_ranges": self.read_ranges,
            "writes": self.writes
        }
        
        try:
            resp = self.cluster.send_to_coordinator(req)
            if resp["status"] == "ok":
                return True
            return False
        except Exception:
            return False
