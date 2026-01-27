import sys
from .base import DatabaseAdapter

try:
    import psycopg2
except ImportError:
    psycopg2 = None

class CRDBAdapter(DatabaseAdapter):
    def __init__(self, dsn="postgresql://root@localhost:26257/defaultdb?sslmode=disable"):
        self.dsn = dsn
        self.conn = None

    def connect(self):
        if not psycopg2:
            raise ImportError("psycopg2 is required for CockroachDB benchmark")
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True

    def close(self):
        if self.conn:
            self.conn.close()

    def setup_schema(self):
        pass

    def execute(self, sql: str) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
            return True
        except Exception as e:
            # print(f"Error: {e}")
            return False

    def clear_data(self):
        self.execute("DROP TABLE IF EXISTS accounts")
        self.execute("DROP TABLE IF EXISTS kv")
