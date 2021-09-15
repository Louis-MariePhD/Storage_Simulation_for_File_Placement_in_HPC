from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment
from collections import OrderedDict
from math import log10
from dataclasses import dataclass, field


@dataclass
class FileCriterias:
    path: str
    C1: float
    C2: float
    C3: float
    C4: float
    Csum : float = field(init=False)

    def __post_init__(self):
        self.Csum = self.C1 + self.C2 + self.C3 + self.C4

    def __lt__(self, other):
        if self.Csum < other.Csum:
            return self.Csum
        else:
            other.Csum
        

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
        self.list_of_all_list_files_criterias = [] # a list containing one dict per on_tier_nearly_full() run

    def on_file_created(self, file: File):
        if file.user not in self.unique_users_capacity_used.keys():
            self.unique_users_capacity_used[file.user] = file.size
        else: 
            self.unique_users_capacity_used[file.user] += file.size

    def on_file_deleted(self, file: File):
        if file.user in self.unique_users_capacity_used.keys():
            self.unique_users_capacity_used[file.user] -= file.size

    def on_file_access(self, file: File, is_write: bool):
        pass # we do nothing ... for now

    def on_tier_nearly_full(self):
        target_tier_id = self.storage.tiers.index(self.tier) + 1  # iterating to the next tier
        if target_tier_id < len(self.storage.tiers):  # checking this next tier do exist
            # Prediction model part
            list_files_criterias = []
            self.biggest_file_on_tier = max([x.size for x in self.tier.content.values()])
            print("biggest_file_on_tier is : {}".format(self.biggest_file_on_tier))
            for file in self.tier.content.values():
                # lifetime criteria, will penalize expired and null lifetimes 
                C1 = (self.env.now - file.creation_time) / (self.prediction_model[file.path] - file.creation_time)
                # size criteria, will penalize big files
                C2 = log10(max(1,file.size)) / log10(max(1, self.biggest_file_on_tier))
                # equity criteria number 1, if user uses tier.target_occupation / number of users this should be equal to 0.1
                C3 = self.unique_users_capacity_used[file.user] / self.tier.target_occupation
                # equity criteria number 2, ideal footprint per user but during a timeframe (last 30 minute in this test, this should be configurable)
                C4 = self.unique_users_capacity_used[file.user] / self.tier.target_occupation
                list_files_criterias.append(FileCriterias(file.path, C1 * self.C1_coeff, C2 * self.C2_coeff, C3 * self.C3_coeff, C4 * self.C4_coeff))
            list_files_criterias.sort(reverse=True)
            i = 0 # not the most pythonic I suppose but hey I'm a C programmer
            while self.tier.used_size > self.tier.max_size * (self.tier.target_occupation - 0.15):
                if len(list_files_criterias) == 0 or len(list_files_criterias) == i:
                    break
                file_to_migrate = self.tier.content[list_files_criterias[i].path] # elements in the list are of type : FileCriterias 
                self.storage.migrate(file_to_migrate, self.storage.tiers[target_tier_id],
                                     self.env.now)
                i+=1

            self.list_of_all_list_files_criterias.append(list_files_criterias) # I can't use pop otherwise i wont get the full list

        else:
            print(f'Tier {self.tier.name} is nearly full, but there is no other tier to discharge load.')
