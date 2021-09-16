import sys
import argparse
import os
import time
import pickle
import simpy
import matplotlib.pyplot as plt
from math import sqrt

if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

from simulation import Simulation
from storage import Tier, StorageManager
from resources import TENCENT_DATASET_FILE_THREAD1

from policies.lru_policy import LRUPolicy
from policies.fifo_policy import FIFOPolicy
from policies.lifetime_overun_policy import LifetimeOverrunPolicy
from policies.random_policy import RandomPolicy
from policies.criteria_based_policy import CriteriaBasedPolicy

from traces.augmented_snia_trace import AugmentedSNIATrace
from traces.custom_trace import CustomTrace
from traces.snia_trace import SNIATrace

available_policies = {"lru": LRUPolicy,
                      "fifo": FIFOPolicy,
                      "lifetime": LifetimeOverrunPolicy,
                      "criteria": CriteriaBasedPolicy,
                      "random": RandomPolicy}
available_traces = {"snia": SNIATrace,
                    "augmented-snia": AugmentedSNIATrace,
                    "custom": CustomTrace}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="Re-enable printing to console during simulation", action="store_true")
    parser.add_argument("-n", "--no-ui", help="Disable opening the figure at the end of a simulation",
                        action="store_true")
    parser.add_argument("-t", "--trace", help="Use a different trace",
                        choices=list(available_traces.keys()), default="augmented-snia")
    parser.add_argument("-p", "--no-progress-bar", help="Disable progress bar", action="store_true", default=False)
    parser.add_argument("-l", "--limit-trace", help="Limit the number of line that will be read from the trace",
                        default="-1", type=int)
    parser.add_argument("-o", "--output-folder", help="The folder in which logs, results and a copy of the config "
                                                      "will be saved", default="logs/<timestamp>", type=str)
    parser.add_argument("-c", "--config-file", help="The config file that will be used for this simulation run. If it "
                                                    "doesn't exist at the given path, a new one will be created and "
                                                    "simulation will exit.",
                        default=os.path.join(os.path.dirname(__file__), "config.cfg"))
    parser.add_argument("policies", nargs='+', choices=["all"] + list(available_policies.keys()))

    args = vars(parser.parse_args())
    verbose, no_ui, custom_trace, no_progress_bar, limit_trace_len, output_folder, config_file, policies = args.values()

    trace_class = available_traces[custom_trace]
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
    unit = 10 ** 11
    number_of_tested_config = 12
    storage_config_list = [[['SSD', round(4 * unit / (sqrt(2) ** (number_of_tested_config - 1 - i))), 100e-6,
                             2e9, 'commandline-policy'],
                            ['HDD', 8 * unit, 10e-3, 250e6, 'commandline-policy'],
                            ['Tapes', 50 * unit, 20, 315e6, 'no-policy']] for i in
                           range(number_of_tested_config)]

    plot_x = []  # storage config str
    plot_y = {}  # policy + stat -> value

    path = TENCENT_DATASET_FILE_THREAD1
    if limit_trace_len != -1:
        path += f'_l{limit_trace_len}'
    if custom_trace:
        path += "_augmented"
    path += '.pickle'

    for storage_config in storage_config_list:
        plot_x += [f'{storage_config[0][0]} {round(storage_config[0][1] / (10 ** 9), 3)} Go']
        for selected_policy in policies:

            # Init simpy env
            env = simpy.Environment()

            # Load trace
            traces = None
            if os.path.exists(path):
                try:
                    with open(path, 'rb') as f:
                        t0 = time.time()
                        print('Loading trace with pickle...', end=' ', flush=True)
                        traces = [pickle.load(f)]
                        print(f'Done after {round((time.time() - t0) * 1000)} ms!')
                except:
                    print("Unable to unpickle file! Deleting pickle file and falling back to slower trace loading.")
                    os.remove(path)

            if traces is None:
                print('Loading trace from file, please wait...')
                t0 = time.time()
                traces = [trace_class(TENCENT_DATASET_FILE_THREAD1, trace_len_limit=limit_trace_len)]
                print(f'Done loading file from file after {round((time.time() - t0) * 1000)} ms!')

                print('Saving to pickle for faster trace load next time...', end=' ', flush=True)
                t0 = time.time()
                with open(path, 'wb') as f:
                    pickle.dump(traces[0], f)
                print(f'Done after {round((time.time() - t0) * 1000)} ms!')

            # Tiers
            tiers = [Tier(*config[:-1]) for config in storage_config]
            storage = StorageManager(tiers, env)

            # Policies
            # No config needed for now, maybe later
            commandline_policy_class = available_policies[selected_policy]
            index = 0
            for config in storage_config:
                policy_str = config[-1]

                policy_class = None
                if policy_str == "no-policy":
                    index += 1
                    continue
                elif policy_str == "commandline-policy":
                    policy_class = commandline_policy_class
                elif policy_str in available_policies.keys():
                    policy_class = available_policies[policy_str]
                if policy_class == LifetimeOverrunPolicy or policy_class == CriteriaBasedPolicy:
                    policy_class(tiers[index], storage, env, traces[0].lifetime_per_fileid)
                else:
                    policy_class(tiers[index], storage, env)

                index += 1

            sim = Simulation(traces, storage, env, log_file=os.path.join(output_folder, "latest.log"),
                             progress_bar_enabled=not no_progress_bar,
                             logs_enabled=verbose)
            print(f'Starting simulation for policy {selected_policy} and storage config {storage_config}!')
            last_results = sim.run()
            last_results = f'{"#" * 10} Run N°{run_index} {"#" * 10}\n{last_results}\n'
            run_index += 1
            print(last_results)
            formatted_results += last_results

            for tier in tiers:
                for stat_name, stat_value in [("Nombre d'io", tier.number_of_reads + tier.number_of_write),
                                              ("Nombre d'io de migration", tier.number_of_prefetching_from_this_tier
                                                                           + tier.number_of_prefetching_to_this_tier
                                                                           + tier.number_of_eviction_from_this_tier
                                                                           + tier.number_of_eviction_to_this_tier),
                                              ("Time spent reading", round(tier.time_spent_reading, 3)),
                                              ("Time spent writing", round(tier.time_spent_writing, 3))]:
                    line_name = f'{selected_policy} - {tier.name} - {stat_name}'
                    if line_name not in plot_y.keys():
                        plot_y[line_name] = []
                    plot_y[line_name] += [stat_value]

    index = 0
    stats_per_config = 4
    tmp = [plt.subplots(1, 1) for i in range(stats_per_config)]
    figs = [v[0] for v in tmp]
    axs = [v[1] for v in tmp]
    colors = [f'C{i}' for i in range(10)]
    markers = ['+', 'x', 's', 'o', 'd']
    storage_tier_count = len(tiers)
    legend = [[] for i in range(stats_per_config)]
    for line_name in plot_y.keys():
        legend[index % stats_per_config] += axs[index % stats_per_config].plot(plot_x, plot_y[line_name],
                                                 f'{colors[int(index / (stats_per_config * storage_tier_count)) % len(colors)]}'
                                                 f'{markers[(int(index / stats_per_config) % storage_tier_count)%len(markers)]}-', label=line_name)
        index += 1
    for i in range(len(figs)):
        axs[i].legend(loc="upper right")
        figs[i].tight_layout()

    fig, axs = plt.subplots(1, 1)
    plt.subplots_adjust(left=0.1)
    axs.axis("tight")
    axs.axis("off")
    table = axs.table(cellText=list(plot_y.values()), rowLabels=list(plot_y.keys()), colLabels=plot_x, loc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(0.8, 0.8)
    #fig.tight_layout()

    try:
        with open(os.path.join(output_folder, "formatted_results.txt"), "w") as f:
            f.write(formatted_results)
        with open(os.path.join(output_folder, "results_display.py"), "w") as f:
            f.write("import matplotlib.pyplot as plt\n\n"
                    f'# plot_x\nplot_x = {plot_x}\n\n'
                    f'# plot_y\nplot_y = {plot_y}\n\n'
                    "index = 0\n"
                    "stats_per_config = 4\n"
                    "tmp = [plt.subplots(1, 1) for i in range(stats_per_config)]\n"
                    "figs = [v[0] for v in tmp]\n"
                    "axs = [v[1] for v in tmp]\n"
                    "colors = [f'C{i}' for i in range(10)]\n"
                    "markers = ['+', 'x', 's', 'o', 'd']\n"
                    "storage_tier_count = len(tiers)\n"
                    "legend = [[] for i in range(stats_per_config)]\n"
                    "for line_name in plot_y.keys():\n"
                    "    legend[index % stats_per_config] += axs[index % stats_per_config].plot(plot_x, plot_y[line_name],\n"
                    "      f'{colors[int(index / (stats_per_config * storage_tier_count)) % len(colors)]}'\n"
                    "      f'{markers[(int(index / stats_per_config) % storage_tier_count) % len(markers)]}-',\n"
                    "      label=line_name)\n"
                    "index += 1\n"
                    "for i in range(len(figs)):\n"
                    "    axs[i].legend(loc='upper right')\n"
                    "figs[i].tight_layout()\n\n"
        
                    "fig, axs = plt.subplots(1, 1)\n"
                    "plt.subplots_adjust(left=0.1)\n"
                    "axs.axis('tight')\n"
                    "axs.axis('off')\n"
                    "table = axs.table(cellText=list(plot_y.values()), rowLabels=list(plot_y.keys()), colLabels=plot_x,\n"
                    "                  loc='center')\n"
                    "table.auto_set_font_size(False)\n"
                    "table.set_fontsize(8)\n"
                    "table.scale(0.8, 0.8)\n")
    except:
        print(f'Error trying to write into a new file in output folder "{output_folder}"')

    if not no_ui:
        plt.show()

    # TODO: ajout de métriques temporelles + vérif des métriques actuelles
    # TODO: ajout d'une option pour ajouter des accès à la trace
    # TODO: tiers dans le fichier de config?
    # TODO: execution parallèle ?
