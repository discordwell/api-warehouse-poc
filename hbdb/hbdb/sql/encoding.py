import json
from typing import Any, List, Tuple, Dict

class KeyEncoder:
    """
    Handles mapping from Table/Ref to KV Keys.
    Format: /t/{table_id}/_r/{row_id}
    """
    
    @staticmethod
    def encode_row(table_id: int, pk_value: Any) -> str:
        # Simple string encoding for now
        return f"/t/{table_id}/_r/{pk_value}"

    @staticmethod
    def decode_row_pk(key: str) -> str:
        # Extract ID from /t/X/_r/ID
        parts = key.split("/")
        return parts[-1]

    @staticmethod
    def encode_row_value(values: Dict[str, Any]) -> str:
        # Simple JSON encoding for row data
        return json.dumps(values)

    @staticmethod
    def decode_row_value(data: str) -> Dict[str, Any]:
        return json.loads(data)

    @staticmethod
    def encode_index(table_id: int, index_id: int, indexed_value: Any, pk_value: Any) -> str:
        """Index key: /t/{table_id}/_i/{index_id}/{indexed_value}/{pk_value}"""
        return f"/t/{table_id}/_i/{index_id}/{indexed_value}/{pk_value}"

    @staticmethod
    def encode_index_prefix(table_id: int, index_id: int, indexed_value: Any) -> str:
        """Prefix for range scan on index."""
        return f"/t/{table_id}/_i/{index_id}/{indexed_value}/"

    @staticmethod
    def decode_index_pk(key: str) -> str:
        """Extract PK from index key."""
        return key.split("/")[-1]
