from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment
from collections import OrderedDict
from math import log10


class CriteriaBasedPolicy(Policy):
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment, prediction_model):
        Policy.__init__(self, tier, storage, env)
        self.unique_users_capacity_used = {} # a dictionary with the capacity used by each user
        self.prediction_model = prediction_model  # is a dictionary containing the lifetime for each file in the trace
        self.biggest_file_on_tier = 0 # in term of size, computed in on_tier_nearly_full()
        self.C1_coeff = 1
        self.C2_coeff = 1
        self.C3_coeff = 1
        self.C4_coeff = 1
        self.C4_timeframe = 60 * 30 # 30 minutes in seconds
        self.list_of_all_files_criterias_runs = [] # a list containing one dict per on_tier_nearly_full() run

    def on_file_created(self, file: File):
        if file.user not in self.unique_users_capacity_used.keys():
            self.unique_users_capacity_used[file.user] = file.size
        else: 
            self.unique_users_capacity_used[file.user] += file.size

    def on_file_deleted(self, file: File):
        if file.path in self.lru_file_dict:  # else we don't need to do anything since it's already not there
            self.lru_file_dict.move_to_end(file.path)
            self.lru_file_dict.popitem()

    def on_file_access(self, file: File, is_write: bool):
        if file.path not in self.lru_file_dict:  # else we don't need to do anything since it's already not there
            if file.path not in file.tier.content:
                print('file is accessed before being created')
                exit()  # somehow this case happened once after i made the modifications, maybe I forgot to save the file before re-running ?
        else:
            self.lru_file_dict.move_to_end(file.path)  # moves it at the end

    def on_tier_nearly_full(self):
        target_tier_id = self.storage.tiers.index(self.tier) + 1  # iterating to the next tier
        if target_tier_id < len(self.storage.tiers):  # checking this next tier do exist
            # Prediction model part
            files_criterias = {} # recreating a new dictionnary each time
            files_criterias_sum = {}
            self.biggest_file_on_tier = max([x for x in self.tier.content.values().size])
            for file in self.tier.content.values():
                # lifetime criteria, will penalize expired and null lifetimes 
                C1 = (self.env.now - file.creation_time) / (self.prediction_model[file.path] - file.creation_time)
                # size criteria, will penalize big files
                C2 = log10(max(1,file.size)) / log10(max(1, self.biggest_file_on_tier))
                # equity criteria number 1, if user uses tier.target_occupation / number of users this should be equal to 0.1
                C3 = self.unique_users_capacity_used[file.user] / self.tier.target_occupation
                # equity criteria number 2, ideal footprint per user but during a timeframe (last 30 minute in this test, this should be configurable)
                C4 = self.unique_users_capacity_used[file.user] / self.tier.target_occupation
                files_criterias[file] = [C1 * self.C1_coeff, C2 * self.C2_coeff, C3 * self.C3_coeff, C4 * self.C4_coeff]
                for k,v in files_criterias:
                    files_criterias_sum[k] = sum(v)

            files_criterias_sum = OrderedDict(sorted(files_criterias_sum.items(), key=lambda item: item[1]))

            while self.tier.used_size > self.tier.max_size * (self.tier.target_occupation - 0.15):
                if len(files_criterias_sum) == 0:
                    break
                self.storage.migrate(files_criterias_sum.popitem(last=True)[0], self.storage.tiers[target_tier_id],
                                     self.env.now)

            self.list_of_all_files_criterias_runs.append(self.files_criterias)

        else:
            print(f'Tier {self.tier.name} is nearly full, but there is no other tier to discharge load.')
