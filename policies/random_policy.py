from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment
import random
from collections import deque


class RandomPolicy(Policy):
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment):
        Policy.__init__(self, tier, storage, env)
        self.rand_list = list() # faster than generating a list from content each time

    def on_file_created(self, file: File):
        self.rand_list.append(file.path)

    def on_file_deleted(self, file: File):
        #self.rand_list.remove(file.path) # we should do that, but it's faster to check if the file exists in self.tier.content 
        pass

    def on_file_access(self, file: File, is_write: bool):
        pass

    def on_tier_nearly_full(self):
        target_tier_id = self.storage.tiers.index(self.tier)+1 # iterating to the next tier
        if target_tier_id < len(self.storage.tiers): # checking this next tier do exist
            while self.tier.used_size > self.tier.max_size * (self.tier.target_occupation - 0.15):
                choice = random.choice(self.rand_list)
                self.rand_list.remove(choice)
                if choice in self.tier.content.keys():
                    # TODO: FIX ME
                    self.storage.migrate(self.tier.content[random.choice(self.rand_list)], self.storage.tiers[target_tier_id], self.env.now) # migrating
        else:
            print(f'Tier {self.tier.name} is nearly full, but there is no other tier to discharge load.')