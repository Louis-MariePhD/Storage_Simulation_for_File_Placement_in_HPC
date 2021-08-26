from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment
from collections import OrderedDict


_DEBUG = False


class LRUPolicy(Policy):
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment):
        Policy.__init__(self, tier, storage, env)
        #self.lru_file_list = list()
        self.lru_file_dict = OrderedDict()

    def on_file_created(self, file: File):
        if _DEBUG:
            print(f'{file.path} created')
        #self.lru_file_list += [file.path]
        self.lru_file_dict[file.path] = file.path

    def on_file_deleted(self, file: File):
        if _DEBUG:
            print(f'{file.path} deleted')
        #self.lru_file_list.remove(file.path)
        if file.path in self.lru_file_dict: # else we don't need to do anything since it's already not there
            self.lru_file_dict.move_to_end(file.path)
            self.lru_file_dict.popitem()

    def on_file_access(self, file: File, is_write: bool):
        if _DEBUG:
            print(f'{file.path} accessed')
        #self.lru_file_list.remove(file.path)
        #self.lru_file_list += [file.path]
        if file.path not in self.lru_file_dict: # else we don't need to do anything since it's already not there
            if file.path not in file.tier.content:
                print('file is accessed before being created')
                exit() # somehow this case happened once after i made the modifications, maybe I forgot to save the file before re-running ? 
        else:
            self.lru_file_dict.move_to_end(file.path) # moves it at the end

    def on_tier_nearly_full(self):
        target_tier_id = self.storage.tiers.index(self.tier)+1 # iterating to the next tier
        if target_tier_id < len(self.storage.tiers): # checking this next tier do exist
            if _DEBUG:
                print(f'Tier {self.tier.name} is nearly full, migrating files'
                f' to {self.storage.tiers[target_tier_id].name}'
                f' \n len of content : {len(self.tier.content)}'
                f' \n len of lru_file_list : {len(self.lru_file_dict)}'
                f' \n used size : {self.tier.used_size}')
            while self.tier.used_size > self.tier.max_size * (self.tier.target_occupation - 0.15):
                # pop the first element
                self.storage.migrate(self.tier.content[self.lru_file_dict.popitem(last=False)[0]], self.storage.tiers[target_tier_id], self.env.now) # migrating
        else:
            print(f'Tier {self.tier.name} is nearly full, but there is no other tier to discharge load.')
