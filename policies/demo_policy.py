from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment


class DemoPolicy(Policy):
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment):
        Policy.__init__(self, tier, storage, env)

    def on_file_created(self, file: File):
        print(f'File {file.path} was created in tier {self.tier.name}')

    def on_file_deleted(self, file: File):
        print(f'File {file.path} was deleted in tier {self.tier.name}')

    def on_file_access(self, file: File, is_write: bool):
        print(f'There was a {["read", "write"][is_write]} in file {file.path} in tier {self.tier.name}')

    def on_tier_nearly_full(self):
        print(f'We were notified of a tier occupation increase in tier {self.tier.name}')
