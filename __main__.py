import sys
if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

import argparse
import os
import time
import pickle
import simpy

from simulation import Simulation
from storage import Tier, StorageManager
from trace import Trace
from traces import TENCENT_DATASET_FILE_THREAD1
from policies.lru_policy import LRUPolicy
from policies.fifo_policy import FIFOPolicy
from policies.lifetime_overun_policy import LRU_LifetimeOverunPolicy
from policies.random_policy import RandomPolicy

available_policies = {"lru" : LRUPolicy,
            "fifo" : FIFOPolicy,
            "lru-lifetime" : LRU_LifetimeOverunPolicy,
            "random": RandomPolicy}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="Re-enable printing to console during simulation", action="store_true")
    parser.add_argument("-p", "--no-progress-bar", help="Disable progress bar", action="store_false")
    parser.add_argument("-o", "--output-folder", help="The folder in which logs, results and a copy of the config "
                                                      "will be saved", default="logs/<timestamp>", type=str)
    parser.add_argument("-c", "--config-file", help="The config file that will be used for this simulation run. If it "
                                                    "doesn't exist at the given path, a new one will be created and "
                                                    "simulation will exit.",
                        default=os.path.join(os.path.dirname(__file__), "config.cfg"))
    parser.add_argument("policies", nargs='+', choices=["all",
                                                        *list(available_policies.keys())])
    args = vars(parser.parse_args())
    verbose, config_file, no_progress_bar, output_folder, policies = args.values()
    if "all" in policies:
        policies = list(available_policies.keys())
        args["policies"] = policies

    print(f'Starting program with parameters {str(args)[1:-1]}') # If you got an error here, you are using python 2.

    if len(policies)>1:
        # branch log dir & start subprocess
        pass
    else:
        # Simpy env
        env = simpy.Environment()

        # Trace
        if os.path.exists(TENCENT_DATASET_FILE_THREAD1 + '.pickle'):
            with open(TENCENT_DATASET_FILE_THREAD1 + '.pickle', 'rb') as f:
                t0 = time.time()
                print('Loading with pickle...', end=' ')
                traces = [pickle.load(f)]
                print('Done after {} ms!')
        else:
            print('Loading from file, please wait...', end=' ')
            traces = [Trace(TENCENT_DATASET_FILE_THREAD1)]
            print('Done after {} ms!')
            print('Saving to pickle for faster trace load next time...', end=' ')
            with open(TENCENT_DATASET_FILE_THREAD1 + '.pickle', 'wb') as f:
                pickle.dump(traces[0], f)
            print('Done after {} ms!')

        # Tiers
        # TODO: Add config here
        tier_ssd = Tier('SSD', 5 * 10 ** 12, 'unknown latency', 'unknown throughput')
        tier_hdd = Tier('HDD', 10 * 10 ** 12, 'unknown latency', 'unknown throughput')
        tier_tapes = Tier('Tapes', 50 * 10 ** 12, 'unknown latency', 'unknown throughput')
        storage = StorageManager([tier_ssd, tier_hdd, tier_tapes], env)

        # Policies
        # No config needed for now, maybe later
        policy = available_policies[policies[0]]
        policy_tier_sdd = policy(tier_ssd, storage, env)
        policy_tier_hdd = policy(tier_hdd, storage, env)

        sim = Simulation(traces, storage, env, os.path.join(output_folder, "latest.log"))
        sim.run()