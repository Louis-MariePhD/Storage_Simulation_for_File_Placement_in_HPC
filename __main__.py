import argparse
import os
import sys
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
    parser.add_argument("-c", "--config-file", help="The config file that will be used for this simulation run. If it "
                                                    "doesn't exist at the given path, a new one will be created and "
                                                    "simulation will exit.",
                        default=os.path.join(os.path.dirname(__file__), "config.cfg"))
    parser.add_argument("policies", nargs='+', choices=["all",
                                                        *list(available_policies.keys())])
    args = vars(parser.parse_args())
    verbose, config_file, no_progress_bar, policies = args.values()
    if "all" in policies:
        policies = list(available_policies.keys())
        args["policies"] = policies

    print(f'Starting program with parameters {str(args)[1:-1]}') # If you got an error here, you are using python 2.

    if len(policies>1):
        # branch log dir & start subprocess
        pass
    else:
        # start simulation normally
        pass