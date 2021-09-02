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

available_policies = {"lru": LRUPolicy,
                      "fifo": FIFOPolicy,
                      "lru-lifetime": LRU_LifetimeOverunPolicy,
                      "random": RandomPolicy}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="Re-enable printing to console during simulation", action="store_true")
    parser.add_argument("-p", "--no-progress-bar", help="Disable progress bar", action="store_true", default=False)
    parser.add_argument("-o", "--output-folder", help="The folder in which logs, results and a copy of the config "
                                                      "will be saved", default="logs/<timestamp>", type=str)
    parser.add_argument("-c", "--config-file", help="The config file that will be used for this simulation run. If it "
                                                    "doesn't exist at the given path, a new one will be created and "
                                                    "simulation will exit.",
                        default=os.path.join(os.path.dirname(__file__), "config.cfg"))
    parser.add_argument("policies", nargs='+', choices=["all"] + list(available_policies.keys()))

    args = vars(parser.parse_args())
    verbose, no_progress_bar, output_folder, config_file, policies = args.values()

    if "all" in policies:
        policies = list(available_policies.keys())
        args["policies"] = policies

    print(f'Starting program with parameters {str(args)[1:-1]}')  # If you got an error here, you are using python 2.

    # Write commandline parameters to logs
    try:
        output_folder = output_folder.replace("<timestamp>",
                                              time.strftime("%a_%d_%b_%Y_%H:%M:%S", time.localtime()))
        os.makedirs(output_folder, exist_ok=True)
        with open(os.path.join(output_folder, "commandline_parameters.txt"), "w") as f:
            f.writelines(str(args)[1:-1].replace(", ", "\n"))
    except:
        print(f'Error trying to write into a new file in output folder "{output_folder}"')

    run_index = 0
    formatted_results = ""
    storage_config_list = [[['SSD', 5 * 10 ** 12, 'unknown latency', 'unknown throughput', 'commandline-policy'],
                            ['HDD', 10 * 10 ** 12, 'unknown latency', 'unknown throughput', 'commandline-policy'],
                            ['Tapes', 50 * 10 ** 12, 'unknown latency', 'unknown throughput', 'no-policy']]]
    for storage_config in storage_config_list:
        for selected_policy in policies:

            # Init simpy env
            env = simpy.Environment()

            # Load trace
            traces = None
            if os.path.exists(TENCENT_DATASET_FILE_THREAD1 + '.pickle'):
                try:
                    with open(TENCENT_DATASET_FILE_THREAD1 + '.pickle', 'rb') as f:
                        t0 = time.time()
                        print('Loading trace with pickle...', end=' ', flush=True)
                        traces = [pickle.load(f)]
                        print(f'Done after {round((time.time() - t0) * 1000)} ms!')
                except:
                    print("Unable to unpickle file! Deleting pickle file and falling back to slower trace loading.")
                    os.remove(TENCENT_DATASET_FILE_THREAD1 + '.pickle')

            if traces is None:
                print('Loading trace from file, please wait...')
                t0 = time.time()
                traces = [Trace(TENCENT_DATASET_FILE_THREAD1)]
                print(f'Done after {round((time.time() - t0) * 1000)} ms!')
                print('Saving to pickle for faster trace load next time...', end=' ', flush=True)
                t0 = time.time()
                with open(TENCENT_DATASET_FILE_THREAD1 + '.pickle', 'wb') as f:
                    pickle.dump(traces[0], f)
                print(f'Done after {round((time.time() - t0) * 1000)} ms!')

            # Tiers
            # TODO: Add config here
            tiers = [Tier(*config[:-1]) for config in storage_config]
            storage = StorageManager(tiers, env)

            # Policies
            # No config needed for now, maybe later
            commandline_policy = available_policies[selected_policy]
            index = 0
            for config in storage_config:
                policy_str = config[-1]
                if policy_str == "no-policy":
                    pass
                elif policy_str == "commandline-policy":
                    commandline_policy(tiers[index], storage, env)
                elif policy_str in available_policies.keys():
                    available_policies[policy_str](tiers[index], storage, env)
                index+=1

            sim = Simulation(traces, storage, env, log_file=os.path.join(output_folder, "latest.log"),
                             progress_bar_enabled=not no_progress_bar,
                             logs_enabled=verbose)
            print(f'Starting simulation for policy {policy_str} and storage config {storage_config}!')
            last_results = sim.run()
            last_results = f'{"#" * 10} Run N°{run_index} {"#" * 10}\n{last_results}\n'
            print(last_results)
            formatted_results += last_results

    try:
        with open(os.path.join(output_folder, "formatted_results.txt"), "w") as f:
            f.write(formatted_results)
    except:
        print(f'Error trying to write into a new file in output folder "{output_folder}"')

    # TODO: ajout de métriques temporelles + vérif des métriques actuelles
    # TODO: ajout d'une option pour ajouter des accès à la trace
    # TODO: ajout de graphe matplotlib exporté en png en fin de simulation
    # TODO: tiers dans le fichier de config?
    # TODO: execution parallèle ?
