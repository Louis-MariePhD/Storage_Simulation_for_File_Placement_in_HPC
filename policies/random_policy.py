from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment
import random


class RandomPolicy(Policy):
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment):
        Policy.__init__(self, tier, storage, env)

    def on_file_created(self, file: File):
        pass

    def on_file_deleted(self, file: File):
        pass

    def on_file_access(self, file: File, is_write: bool):
        pass

    def on_tier_nearly_full(self):
        target_tier_id = self.storage.tiers.index(self.tier)+1 # iterating to the next tier
        if target_tier_id < len(self.storage.tiers): # checking this next tier do exist
            while self.tier.used_size > self.tier.max_size * self.tier.target_occupation:
                self.storage.migrate(random.choice(self.tier.content.keys()), self.storage.tiers[target_tier_id]) # migrating
        else:
            print(f'Tier {self.tier.name} is nearly full, but there is no other tier to discharge load.')