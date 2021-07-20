from policies.policy import Policy
from storage import StorageManager, File, Tier
from simpy.core import Environment
from reinforcement_learning.ddgd import DDGD
import math

import reinforcement_learning.utils as utils

"""
RL_LRU_LO: (Least Recently Used + Lifetime Overrun) In addition to a standard LRU policy, we mark for migration files
whose lifetimes are finished. We use an RL lifetime prediction model.

RL_LRL: (Least Remaining Lifetime) We first migrate files whose lifetime are the most advanced (either as a % or as a flat value)

RL_KFP: (Metric prediction, knapsack problem file placement) We use RL to predict the performance gain we get by keeping each
file in the tier. We then use a knapsack problem solver to optimise the performance chosen metric given a disk size.
Ref for the knapsack problem:
    - https://www.researchgate.net/publication/220694474_Knapsack_Problems
    - https://en.wikipedia.org/wiki/Knapsack_problem

RL_TKFP: (Temporal Metric prediction, temporal knapsack problem file placement) We use RL to predict the performance gain
over discrete time intervals (ex. 0-1h, 1h-6h, 6h-12h, 12h-24h, 24h+). Thus, we get an optimisation problem similar to
the temporal knapsack problem. Solving this problem would allow a system to both load and unload files from a tier.
Ref for the temporal knapsack problem:
    - https://www.cs.york.ac.uk/aig/constraints/Grid/cpaior05.pdf
"""


def reward(predicted_lifetime, real_lifetime):
    """
    Reward function. Could be R(state, action, state+1). State being the full meta of the file, containing creation_time,
    last_mod, last_access, path. The path that is the sole input of the agent being an observation.
    :param predicted_lifetime: output of the regression model
    :param real_lifetime: real value
    :return: the reward
    """

    # return 1-abs(math.log10(max(1,predicted_lifetime)/max(1,real_lifetime)))
    # return 1 - abs((predicted_lifetime-real_lifetime)/real_lifetime)
    return 1 - math.log10(1 + abs((predicted_lifetime - real_lifetime) / real_lifetime))


class DDGD_LRU_LO_Policy(Policy):
    """
    (Least Recently Used + Lifetime Overrun)
    In addition to a standard LRU policy, we mark for migration files whose lifetimes are finished. We use an RL
    lifetime prediction model. Lifetime is defined as the earliest date after which the file is not used for a given
    time, by default a week.

    Placement Strategy:
        - We predict lifetimes when files are created on the tier, once and for all
        - When the tier is full, we remove files whose lifetime have ended for the longest time.
        - When all files are 'alives', we use LRU.

    Learning Strategy:
        - We add the real lifetime to the replay memory when a file is deleted.
        - For the model to learn, we have to get a feedback on the prediction even when a file is never deleted.
          Since we defined the end of the lifetime of a file as when a file is inactive for a given time (eg. 1 week),
          we create a daily process that detect such inactive files, use their last_access stat as their lifetimes,
          and add them to the replay memory. The regression agent is updated in this (daily) process.

    Improvements done:
        - When a file is inactive, we use its last access - that is more accurate - instead of its predicted
          lifetime for file removal.

    TODO:
        - We could update the lifetime prediction daily. We would then be able to add last_access-creation_time,
          last_mod-creation_time etc to the input of the regression model. At creation time, theses values are always 0,
          that's why we have not used these meaningful metadata yet. We would need a 'real' trace though.
        - Adding last_access etc to the input of the regression model would enable us to handle the case of files going
          up in the tier, and becoming active again a long time after the end of their lifetime.
        - Separate file creation and file arriving in the tier. It's not the same! One occurs once, the other can occurs
          multiple times. Same for file deletion / file migration.
        - Create a good ol' DQN version
    """
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment, evaluation_period=60*60*24,
                 evaluate_as_inactive_after=7*24*3600):
        # LRU Policy
        Policy.__init__(self, tier, storage, env)
        self.lru_file_list = list()

        # LO Policy
        self.regression_agent = DDGD("actor", "actor_target", "critic", "critic_target")
        self.prediction_data = {}  # we add the files that are awaiting evaluation here
        self.next_batch_size = 0  # we update the model once every time a file is added to the history.
        if evaluate_as_inactive_after > 0 and evaluation_period > 0:
            self.evaluation = env.process(self, self.daily_process(evaluation_period, evaluate_as_inactive_after))

    def daily_process(self, period=60 * 60 * 24, evaluate_as_inactive_after=7 * 24 * 3600):
        while True:
            # Updating agent
            self.regression_agent.update(self.next_batch_size)
            self.next_batch_size = 0

            # Evaluation
            for file, data in self.prediction_data.items():
                if file.last_access + evaluate_as_inactive_after > self.env.now:
                    self.regression_agent.memory.push(state=data["state"],
                                                      action=data["prediction"],
                                                      reward=reward(data["prediction"],
                                                                    self.env.now - file.creation_time),
                                                      next_state=None, done=True)
                    self.next_batch_size += 1
                    data["inactive_since"] = file.last_access
                else:
                    data["inactive_since"] = None

            # Sleeping until next day
            yield self.env.timeout(period)

    def on_file_created(self, file: File):
        # LRU Policy
        self.lru_file_list += [file]
        state = utils.str2array(file.path)

        # LO Policy
        self.prediction_data[file] = {"prediction":self.regression_agent.get_action(state),  # dict slower but readable
                                      "state": utils.str2array(file.path),
                                      "inactive_since":None}

    def on_file_deleted(self, file: File):
        # LRU Policy
        self.lru_file_list.remove(file)

        # LO Policy
        data = self.prediction_data.pop(file)
        self.regression_agent.memory.push(state=data["state"],
                                          action=data["prediction"],
                                          reward=reward(data["prediction"],
                                                        self.env.now - file.creation_time),
                                          next_state=None, done=True)
        self.next_batch_size += 1

    def on_file_access(self, file: File, is_write: bool):
        # LRU Policy
        self.lru_file_list.remove(file)
        self.lru_file_list += [file]

    def on_tier_nearly_full(self):
        target_tier_id = self.storage.tiers.index(self.tier)+1  # iterating to the next tier
        self.prediction_data = dict(sorted(self.prediction_data.items(), # sorting predicted lifetimes
                                           key=lambda item: item[0].creation_time+
                                                            [item[1]["inactive_since"],
                                                             item[1]["prediction"]][item[1]["inactive_since"] is None]))

        if target_tier_id < len(self.storage.tiers):  # checking if the next tier do exist
            while self.tier.used_size > self.tier.max_size * self.tier.target_occupation:
                # LO Policy
                if len(self.prediction_data.keys()>0):
                    file, data = list(self.prediction_data.items())[0]
                    if file.creation_time + data["lifetime"] < self.env.now:
                        self.storage.migrate(file, self.storage.tiers[target_tier_id])
                        continue

                # When the LO Policy is not applicable (all files alive), LRU Policy is applied
                self.storage.migrate(self.lru_file_list[0], self.storage.tiers[target_tier_id])

                # In both policies, file will be removed from the FIFO list in the 'delete' event during migration
        else:
            print(f'Tier {self.tier.name} is nearly full, but there is no other tier to discharge load.'
                  'RIP storage system.')
