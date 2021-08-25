from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment
import time


class LRU_LifetimeOverunPolicy(Policy):
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment, prediction_model):
        Policy.__init__(self, tier, storage, env)
        self.lru_file_list = list()
        self.prediction_model = prediction_model

    def on_file_created(self, file: File):
        self.lru_file_list += [file]

    def on_file_deleted(self, file: File):
        self.lru_file_list.remove(file)

    def on_file_access(self, file: File, is_write: bool):
        self.lru_file_list.remove(file)
        self.lru_file_list += [file]

    def on_tier_nearly_full(self):
        target_tier_id = self.storage.tiers.index(self.tier)+1 # iterating to the next tier
        if target_tier_id < len(self.storage.tiers): # checking this next tier do exist
            print(f'Tier {self.storage.tiers[target_tier_id-1].name} is nearly full, migrating files '
                  f'to {self.storage.tiers[target_tier_id].name}!')
            t = time.time()
            expired_files = []
            print("Listing files with expired lifetime... ", end='')
            for file in self.tier.content:
                remaining_lt = self.env.now - file.creation_time- self.prediction_model(file.path)
                if remaining_lt > 0:
                    expired_files += [(remaining_lt, file)]
            expired_files = [file for _, file in sorted(expired_files)]
            print(f'done after {round((time.time()-t)*1000)} ms!')

            while self.tier.used_size > self.tier.max_size * (self.tier.target_occupation - 0.15):
                self.storage.migrate(expired_files.pop(-1), self.storage.tiers[target_tier_id])

            while self.tier.used_size > self.tier.max_size * (self.tier.target_occupation - 0.15):
                self.storage.migrate(self.lru_file_list.pop(0), self.storage.tiers[target_tier_id]) # migrating
        else:
            print(f'Tier {self.tier.name} is nearly full, but there is no other tier to discharge load.')
