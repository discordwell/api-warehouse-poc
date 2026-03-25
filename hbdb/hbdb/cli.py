import cmd
import sys
import time
from hbdb.db import HBDB
from hbdb.sql.engine import SQLEngine
from typing import List, Dict, Any

class HBDBShell(cmd.Cmd):
    intro = 'Welcome to the HoneyBadgerDB SQL Shell. Type help or ? to list commands.\n'
    prompt = 'hbdb> '

    def __init__(self, host=None, port=None):
        super().__init__()
        connect_str = f"{host}:{port}" if host and port else None
        
        if connect_str:
            print(f"Connecting to {connect_str}...")
        else:
            print("Initializing HBDB (Embedded)...")
            
        self.db = HBDB(connect_to=connect_str)
        self.engine = SQLEngine(self.db)
        print("Ready.")

    def do_exit(self, arg):
        """Exit the shell."""
        print("Bye!")
        return True
    
    def do_quit(self, arg):
        return self.do_exit(arg)

    def default(self, line):
        """Execute SQL statement."""
        if line == "EOF":
            return True
            
        start = time.time()
        try:
            results = self.engine.execute(line)
            duration = (time.time() - start) * 1000
            
            self._print_results(results)
            print(f"({len(results)} rows, {duration:.2f}ms)")
            
        except Exception as e:
            print(f"Error: {e}")

    def _print_results(self, results: List[Dict[str, Any]]):
        if not results:
            return
            
        # Collect columns
        columns = list(results[0].keys())
        
        # Calculate widths
        widths = {c: len(c) for c in columns}
        for row in results:
            for c in columns:
                val = str(row.get(c))
                widths[c] = max(widths[c], len(val))
        
        # Header
        header = " | ".join(f"{c:<{widths[c]}}" for c in columns)
        print("-" * len(header))
        print(header)
        print("-" * len(header))
        
        # Rows
        for row in results:
            line = " | ".join(f"{str(row.get(c)):<{widths[c]}}" for c in columns)
            print(line)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", default=9000)
    args = parser.parse_args()
    
    try:
        shell = HBDBShell(args.host, args.port)
        shell.cmdloop()
    except KeyboardInterrupt:
        print("\nBye!")

if __name__ == '__main__':
    main()
