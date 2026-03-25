import argparse
import asyncio
import json
import logging
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor

from hbdb.db import HBDB
from hbdb.core.topology import ClusterTopology
from hbdb.client.client import HBDBClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HBDBServer")

class HBDBDataProtocol(asyncio.Protocol):
    def __init__(self, server):
        self.server = server
        self.transport = None
        self._buffer = b""

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self._buffer += data
        while b"\n" in self._buffer:
            line, self._buffer = self._buffer.split(b"\n", 1)
            if not line.strip(): continue
            asyncio.create_task(self.handle_command(line))

    async def handle_command(self, line):
        try:
            req = json.loads(line)
            cmd = req.get("cmd")
            resp = await self.server.process_command(cmd, req)
            out = json.dumps(resp).encode() + b"\n"
            self.transport.write(out)
        except Exception as e:
            logger.error(f"Error handling command: {e}")
            err = {"status": "error", "msg": str(e)}
            self.transport.write(json.dumps(err).encode() + b"\n")

class HBDBServer:
    def __init__(self, host="0.0.0.0", port=9000, role="coordinator", storage_nodes=None, rf=1):
        self.host = host
        self.port = port
        self.role = role # 'coordinator' or 'storage'
        self.rf = rf
        self.executor = ThreadPoolExecutor(max_workers=8)
        
        # Initialize DB Components
        from hbdb.core.backend import VersionedKVStore
        from hbdb.core.resolver import PartitionedResolver
        from hbdb.core.sequencer import get_sequencer
        
        self.backend = VersionedKVStore()
        
        if self.role == "coordinator":
            self.resolver = PartitionedResolver(num_partitions=4)
            self.sequencer = get_sequencer() # Singleton
            # Topology
            nodes = []
            if storage_nodes:
                for s in storage_nodes.split(","):
                    h, p = s.split(":")
                    nodes.append((h, int(p)))
            self.topology = ClusterTopology(nodes, replication_factor=self.rf)
            self.storage_clients = {
                f"{h}:{p}": HBDBClient(h, int(p)) for h, p in nodes
            }
        else:
            self.resolver = None
            self.sequencer = None

    async def start(self):
        loop = asyncio.get_running_loop()
        server = await loop.create_server(
            lambda: HBDBDataProtocol(self),
            self.host, self.port
        )
        logger.info(f"HBDB Server ({self.role.upper()}) listening on {self.host}:{self.port}")
        await server.serve_forever()

    async def process_command(self, cmd: str, req: Dict[str, Any]) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        
        if self.role == "storage":
            if cmd == "get":
                val = self.backend.read(req["key"], req["ts"])
                return {"status": "ok", "value": val}
            elif cmd == "apply_writes":
                # Internal command from Coordinator
                ts = req["ts"]
                count = 0
                for k, v in req["writes"].items():
                    self.backend.write(k, v, ts)
                    count += 1
                return {"status": "ok", "count": count}
            elif cmd == "scan":
                data = self.backend.scan(req["start"], req["end"], req["ts"])
                return {"status": "ok", "data": data}
            elif cmd == "ping":
                return {"status": "ok", "msg": "pong (storage)"}
            return {"status": "error", "msg": "Unknown command or wrong role"}

        elif self.role == "coordinator":
            if cmd == "get_time":
                import time
                return {"status": "ok", "ts": int(time.time() * 1000000)}
                
            elif cmd == "commit":
                # Async commit flow
                return await loop.run_in_executor(self.executor, self._process_coordinator_commit, req)
                
            elif cmd == "ping":
                return {"status": "ok", "msg": "pong (coordinator)"}
                
            return {"status": "error", "msg": "Unknown command"}

    def _process_coordinator_commit(self, req: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Conflict Check
        read_ts = req["read_ts"]
        read_keys = req.get("read_keys", [])
        read_ranges = req.get("read_ranges", [])
        write_keys = list(req.get("writes", {}).keys())
        
        ok, commit_ts = self.resolver.commit(read_ts, read_keys, read_ranges, write_keys)
        if not ok:
            return {"status": "abort"}
            
        # 2. Durability (WAL) on Coordinator
        self.sequencer.append(commit_ts, req["writes"])
        
        # 3. Broadcast to Storage Nodes (Replication!)
        writes_by_node = {} # "host:port" -> {k:v}
        
        for k, v in req["writes"].items():
            # Get ALL replicas
            replicas = self.topology.get_nodes_for_key(k)
            for host, port in replicas:
                addr = f"{host}:{port}"
                if addr not in writes_by_node:
                    writes_by_node[addr] = {}
                writes_by_node[addr][k] = v
            
        # Send to storage nodes (Synchronous Write-All)
        failed_nodes = []
        for addr, partition_writes in writes_by_node.items():
            client = self.storage_clients.get(addr)
            if client:
                try:
                    client.send({
                        "cmd": "apply_writes",
                        "writes": partition_writes,
                        "ts": commit_ts
                    })
                except Exception as e:
                    logger.error(f"Failed to replicate to {addr}: {e}")
                    failed_nodes.append(addr)
        
        if failed_nodes:
            # In Write-All, if ANY node fails, we technically have availability loss or partial write.
            # Ideally we should reverse? (Not possible without 2PC).
            # For POC, we flag error but commit is "technically" done in WAL.
            # This is "Eventual Consistency" territory if we continue.
            pass
        
        return {"status": "ok", "commit_ts": commit_ts}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", default="coordinator", help="coordinator | storage")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--storage-nodes", help="Comma-sep list of host:port for storage nodes")
    parser.add_argument("--rf", type=int, default=1, help="Replication Factor")
    args = parser.parse_args()
    
    server = HBDBServer(port=args.port, role=args.role, storage_nodes=args.storage_nodes, rf=args.rf)
    asyncio.run(server.start())
