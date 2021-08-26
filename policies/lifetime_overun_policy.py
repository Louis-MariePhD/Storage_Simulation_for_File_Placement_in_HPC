from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment
import time
from collections import OrderedDict



class LRU_LifetimeOverunPolicy(Policy):
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment, prediction_model):
        Policy.__init__(self, tier, storage, env)
        self.lru_file_dict = OrderedDict()
        #self.lru_file_list = list()
        self.prediction_model = prediction_model # is a dictonary

    def on_file_created(self, file: File):
        self.lru_file_dict[file.path] = file.path

    def on_file_deleted(self, file: File):
        if file.path in self.lru_file_dict: # else we don't need to do anything since it's already not there
            self.lru_file_dict.move_to_end()
            self.lru_file_dict.popitem()

    def on_file_access(self, file: File, is_write: bool):
        if file.path not in self.lru_file_dict: # else we don't need to do anything since it's already not there
            if file.path not in file.tier.content:
                print('file is accessed before being created')
                exit() # somehow this case happened once after i made the modifications, maybe I forgot to save the file before re-running ? 
        else:
            self.lru_file_dict.move_to_end(file.path) # moves it at the end

    def on_tier_nearly_full(self):
        target_tier_id = self.storage.tiers.index(self.tier)+1 # iterating to the next tier
        if target_tier_id < len(self.storage.tiers): # checking this next tier do exist
            print(f'Tier {self.storage.tiers[target_tier_id-1].name} is nearly full, migrating files '
                  f'to {self.storage.tiers[target_tier_id].name}!')
            # Prediction model part
            t = time.time()
            expired_files = []
            print("Listing files with expired lifetime... ", end='')
            for file in self.tier.content.values():
                remaining_lt = self.env.now - file.creation_time- self.prediction_model(file.path)
                if remaining_lt > 0:
                    expired_files += [(remaining_lt, file)]
            expired_files = [file for _, file in sorted(expired_files)]
            print(f'done after {round((time.time()-t)*1000)} ms!')

            while self.tier.used_size > self.tier.max_size * (self.tier.target_occupation - 0.15):
                self.storage.migrate(expired_files.pop(-1), self.storage.tiers[target_tier_id])

            while self.tier.used_size > self.tier.max_size * (self.tier.target_occupation - 0.15):
                # pop the first element
                self.storage.migrate(self.tier.content[self.lru_file_dict.popitem(last=False)[0]], self.storage.tiers[target_tier_id], self.env.now) # migrating

        else:
            print(f'Tier {self.tier.name} is nearly full, but there is no other tier to discharge load.')
