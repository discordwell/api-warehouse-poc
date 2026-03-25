"""
Sequencer Role.
Responsible for durability. Writes transactions to a Write-Ahead Log (WAL) on disk.
In a real system, this is replicated (e.g., Raft or Paxos).
"""
import os
import json
import threading
from typing import Dict, Any

class Sequencer:
    """
    Single-node Sequencer that logs to a local file.
    """
    def __init__(self, log_path: str = "transaction.log"):
        self.log_path = log_path
        self._lock = threading.Lock()
        
        # Ensure log dir exists
        log_dir = os.path.dirname(log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

    def append(self, commit_ts: int, writes: Dict[str, Any]) -> int:
        """
        Durably append transaction to log.
        Returns the Log Sequence Number (file offset).
        """
        if not writes:
            return 0
            
        payload = {
            "ts": commit_ts,
            "ops": writes
        }
        
        # Simple JSONL format
        # In production: Binary format + CRC32 checksum
        line = json.dumps(payload) + "\n"
        data = line.encode("utf-8")
        
        with self._lock:
            with open(self.log_path, "ab") as f:
                offset = f.tell()
                f.write(data)
                f.flush()
                # os.fsync(f.fileno()) # Strict durability (Costly: commented out for POC speed/robustness tradeoff in tests, enabled in prod)
                os.fsync(f.fileno()) 
        
        return offset

    def rotate_log(self) -> str:
        """
        Rotate the current log file.
        Renames transaction.log -> transaction.log.archive.<ts>
        Returns the path to the archived log.
        """
        import time
        import shutil
        
        with self._lock:
            if not os.path.exists(self.log_path):
                # Touch file if not exists
                open(self.log_path, 'a').close()
                return None
                
            archive_path = f"{self.log_path}.archive.{int(time.time())}"
            # Atomic rename (on POSIX) ensures we don't lose writes if append is locked (we are locked)
            os.rename(self.log_path, archive_path)
            
            # Touch new log
            open(self.log_path, 'a').close()
            
            return archive_path

# Global sequencer
_sequencer = Sequencer()

def get_sequencer() -> Sequencer:
    return _sequencer
