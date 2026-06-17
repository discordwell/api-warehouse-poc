from .core.backend import VersionedKVStore
from .core.resolver import Resolver, PartitionedResolver
from .core.proxy import Transaction
from .core.cache import LRUCache
from typing import Any

class HBDB:
    """
    HBDB: FoundationDB-style Unbundled Architecture.
    """
    def __init__(self, num_partitions: int = 4, connect_to: str = None, rf: int = 1,
                 force_python: bool = False):
        """
        :param connect_to: Connection string.
                           Simple: "host:port" (Single/Coordinator)
                           Cluster: "coord_host:port;store1:port,store2:port"
        :param rf: Replication Factor.
        :param force_python: Skip the C++ native extension even if built
                             (pure-Python backend + resolver). Local mode
                             only; ignored when connect_to is set.
        """
        self.remote_addr = connect_to
        # Per-database read cache for the SQL layer. Scoping it to the
        # instance (rather than a process-wide singleton) keeps two HBDBs in
        # the same process from colliding on identical storage keys.
        self.read_cache = LRUCache()
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
            self.backend = VersionedKVStore(force_python=force_python)
            self.resolver = PartitionedResolver(num_partitions=num_partitions,
                                                force_python=force_python)
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
        snapshot_ts = 0

        if os.path.exists(snapshot_path):
            print(f"[HBDB] Loading snapshot from {snapshot_path}...")
            # Native restoration
            snapshot_ts = self.backend.load_snapshot(snapshot_path)
            self.resolver.restore_clock(snapshot_ts)
            print(f"[HBDB] Snapshot loaded. Clock at {snapshot_ts}.")

        # 2. Replay Logs.
        # Archive files exist only if take_snapshot() crashed after
        # rotating the log: their commits never made it into a snapshot,
        # so replay them (oldest first) before the live log. Re-applying
        # an entry the snapshot already holds is idempotent either way.
        def archive_order(path):
            suffix = path.rsplit(".", 1)[-1]
            return int(suffix) if suffix.isdigit() else 0

        log_paths = sorted(glob.glob("transaction.log.archive.*"), key=archive_order)
        log_paths.append("transaction.log")

        count = 0
        skipped = 0
        max_ts = snapshot_ts
        found_log = False

        for log_path in log_paths:
            if not os.path.exists(log_path):
                continue
            found_log = True
            print(f"[HBDB] Replaying WAL from {log_path}...")

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

                    # Skip entries the snapshot already covers. Compare against
                    # the fixed snapshot baseline, not the running max: commits
                    # can land in the WAL slightly out of timestamp order, and
                    # an out-of-order entry is still durable data.
                    if ts <= snapshot_ts:
                        continue

                    # Apply to backend
                    for k, v in ops.items():
                        self.backend.write(k, v, ts)

                    if ts > max_ts:
                        max_ts = ts
                    count += 1

        if not found_log:
            return

        # Restore Clock again in case the logs moved it forward
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
