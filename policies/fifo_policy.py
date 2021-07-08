from policies.lru_policy import LRUPolicy
from storage import StorageManager, File, Tier
from simpy.core import Environment


class FIFOPolicy(LRUPolicy):
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment):
        LRUPolicy.__init__(self, tier, storage, env)

    def on_file_access(self, file: File, is_write: bool):
        pass # FIFO is similar to LRU, at the difference we don't push up file when they are accessed. Thus the override
