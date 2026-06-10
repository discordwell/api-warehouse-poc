from .core.backend import VersionedKVStore
from .core.resolver import Resolver, PartitionedResolver
from .core.proxy import Transaction
from typing import Any

class HBDB:
    """
    HBDB: FoundationDB-style Unbundled Architecture.
    """
    def __init__(self, num_partitions: int = 4, connect_to: str = None, rf: int = 1):
        """
        :param connect_to: Connection string.
                           Simple: "host:port" (Single/Coordinator)
                           Cluster: "coord_host:port;store1:port,store2:port"
        :param rf: Replication Factor.
        """
        self.remote_addr = connect_to
        if self.remote_addr:
            from hbdb.client.client import ClusterClient, HBDBClient
            
            if ";" in connect_to:
                coord_str, store_str = connect_to.split(";")
                store_list = store_str.split(",")
                self.client = ClusterClient(coord_str, store_list, rf=rf)
            else:
                # V5 Compatibility: Single Node acts as both
                self.client = ClusterClient(connect_to, [connect_to], rf=1)

            # Backend/Resolver are virtual in remote mode
            self.backend = None 
            self.resolver = None
        else:
            self.backend = VersionedKVStore()
            self.resolver = PartitionedResolver(num_partitions=num_partitions)
            self.recover()

    def recover(self):
        """
        Recover state from Snapshot + Transaction Log (WAL).
        """
        import os
        import json
        import glob
        
        # 1. Load Snapshot
        snapshot_path = "snapshot.bin"
        max_ts = 0
        
        if os.path.exists(snapshot_path):
            print(f"[HBDB] Loading snapshot from {snapshot_path}...")
            # Native restoration
            max_ts = self.backend.load_snapshot(snapshot_path)
            self.resolver.restore_clock(max_ts)
            print(f"[HBDB] Snapshot loaded. Clock at {max_ts}.")

        # 2. Replay Log
        log_path = "transaction.log"
        if not os.path.exists(log_path):
            return

        print(f"[HBDB] Replaying WAL from {log_path}...")
        count = 0
        skipped = 0

        with open(log_path, "r") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    ts = entry["ts"]
                    ops = entry["ops"]
                    if not isinstance(ts, int) or not isinstance(ops, dict):
                        raise TypeError("malformed WAL entry")
                except (json.JSONDecodeError, KeyError, TypeError):
                    # A torn final line is expected after a crash mid-append;
                    # anything else corrupt is skipped but counted.
                    skipped += 1
                    continue

                if ts <= max_ts:
                    continue # Skip already snapshotted

                # Apply to backend
                for k, v in ops.items():
                    self.backend.write(k, v, ts)

                max_ts = ts
                count += 1

        # Restore Clock again in case log moved it forward
        self.resolver.restore_clock(max_ts)
        suffix = f" ({skipped} corrupt lines skipped)" if skipped else ""
        print(f"[HBDB] Replayed {count} transactions from WAL. Clock at {max_ts}.{suffix}")

    def take_snapshot(self):
        """
        Atomically take a snapshot and truncate the log.
        """
        import os
        from hbdb.core.sequencer import get_sequencer
        
        print("[HBDB] Starting Snapshot...")
        
        # 1. Rotate Log
        # New writes go to new log. Old writes (up to now) are in archive.
        archive_path = get_sequencer().rotate_log()
        
        # 2. Save Snapshot
        # This blocks writes to backend, ensuring we capture everything up to the rotation point
        # (and possibly a bit more if race condition favors new log, but that's fine).
        # Actually, since we rotated FIRST, any concurrent write might end up in new log
        # AND in snapshot (if it grabs backend lock before snapshot).
        # That's idempotent, so it's safe.
        self.backend.save_snapshot("snapshot.bin.tmp")
        os.rename("snapshot.bin.tmp", "snapshot.bin")
        
        # 3. Cleanup Archive
        if archive_path and os.path.exists(archive_path):
            os.remove(archive_path)
            
        print("[HBDB] Snapshot complete. Log truncated.")

    def transaction(self):
        """Create a new interactive transaction."""
        if self.remote_addr:
            from hbdb.client.txn import NetworkTransaction
            return NetworkTransaction(self.client)
        return Transaction(self.backend, self.resolver)

    def set_sync(self, key: str, value: Any):
        """Synchronously set a value (atomic transaction)."""
        tx = self.transaction()
        tx.set(key, value)
        if not tx.commit():
            raise RuntimeError("Sync set failed")

    def get_sync(self, key: str) -> Any:
        """Synchronously get a value."""
        tx = self.transaction()
        return tx.get(key)
