from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment


class LRUPolicy(Policy):
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment):
        Policy.__init__(self, tier, storage, env)
        self.lru_file_list = list()

    def on_file_created(self, file: File):
        print(f'{file.path} created')
        self.lru_file_list += [file.path]

    def on_file_deleted(self, file: File):
        print(f'{file.path} deleted')
        self.lru_file_list.remove(file.path)

    def on_file_access(self, file: File, is_write: bool):
        print(f'{file.path} accessed')
        #print(self.lru_file_list)
        #print(len(self.lru_file_list))
        #print(file.path)
        #print(type(file.path))
        self.lru_file_list.remove(file.path)
        self.lru_file_list += [file.path]

    def on_tier_nearly_full(self):
        target_tier_id = self.storage.tiers.index(self.tier)+1 # iterating to the next tier
        if target_tier_id < len(self.storage.tiers): # checking this next tier do exist
            print(f'Tier {self.tier.name} is nearly full, migrating files'
            f' to {self.storage.tiers[target_tier_id].name}'
            f' \n len of content : {len(self.tier.content)}'
            f' \n len of lru_file_list : {len(self.lru_file_list)}'
            f' \n used size : {self.tier.used_size}')

            while self.tier.used_size > self.tier.max_size * self.tier.target_occupation:
                self.storage.migrate(self.tier.content[self.lru_file_list[0]], self.storage.tiers[target_tier_id], self.env.now) # migrating
        else:
            print(f'Tier {self.tier.name} is nearly full, but there is no other tier to discharge load.')
