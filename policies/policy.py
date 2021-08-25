from storage import StorageManager, File, Tier
from simpy.core import Environment


class Policy:
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment):
        """
        :param storage:
        :param env:
        """
        self.tier = tier
        self.storage = storage
        self.env = env
        tier.register_listener(self)

    def on_file_created(self, file: File):
        pass

    def on_file_deleted(self, file: File):
        pass

    def on_file_access(self, file: File, is_write: bool):
        pass

    def on_tier_nearly_full(self):
        pass
