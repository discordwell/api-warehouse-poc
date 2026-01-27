from .core.backend import VersionedKVStore
from .core.resolver import Resolver, PartitionedResolver
from .core.proxy import Transaction

class HBDB:
    """
    HBDB: FoundationDB-style Unbundled Architecture.
    """
    def __init__(self, num_partitions: int = 4):
        self.backend = VersionedKVStore()
        self.resolver = PartitionedResolver(num_partitions=num_partitions)

    def transaction(self) -> Transaction:
        """Create a new interactive transaction."""
        return Transaction(self.backend, self.resolver)
