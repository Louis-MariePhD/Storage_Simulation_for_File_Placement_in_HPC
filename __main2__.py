import sys
import os
import time
import simpy

from traces.ibm_object_store_trace import IBMObjectStoreTrace

if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

from simulation import Simulation
from storage import Tier, StorageManager

from policies.lru_policy import LRUPolicy
from policies.fifo_policy import FIFOPolicy

trace = IBMObjectStoreTrace()
trace.gen_data(trace_len_limit=10000)

output_folder = "logs/<timestamp>"
output_folder = output_folder.replace('/',os.path.sep).replace("<timestamp>",
                                      time.strftime("%a_%d_%b_%Y_%H-%M-%S", time.localtime()))
output_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), output_folder))
try:
    os.makedirs(output_folder, exist_ok=True)
except:
    print(f'Error trying to create output folder "{output_folder}"')

# Create the storage tiers
ssd = Tier(name="SSD", max_size=2*10**12, latency=100e-6, throughput=2e9, target_occupation=0.9)
hdd = Tier(name="HDD", max_size=8*10**12, latency=10e-3, throughput=250e6, target_occupation=0.9)
tapes = Tier(name="Tapes", max_size=20*10**12, latency=20.0, throughput=315e6, target_occupation=0.9)

# Init simpy env
env = simpy.Environment()

# The storage manager is an utility object giving info on the tier ordering & default tier
tiers = [ssd, hdd, tapes]
storage = StorageManager(tiers, env)

# Creating the eviction policies
policy_SSD = LRUPolicy(ssd, storage, env)
policy_HDD = FIFOPolicy(hdd, storage, env)

sim = Simulation([trace], storage, env, log_file=os.path.join(output_folder, "latest.log"),
                 progress_bar_enabled=False,
                 logs_enabled=True)

print("Starting simulation")

last_results = sim.run()
last_results = f'{"#" * 10} Run {"#" * 10}\n{last_results}\n'

for tier in tiers:
    for stat_name, stat_value in [("Nombre d'io", tier.number_of_reads + tier.number_of_write),
                                  ("Nombre d'io de migration", tier.number_of_prefetching_from_this_tier
                                                               + tier.number_of_prefetching_to_this_tier
                                                               + tier.number_of_eviction_from_this_tier
                                                               + tier.number_of_eviction_to_this_tier),
                                  ("Time spent reading", round(tier.time_spent_reading, 3)),
                                  ("Time spent writing", round(tier.time_spent_writing, 3))]:
        line_name = f'{tier.name} - {stat_name}'

try:
    with open(os.path.join(output_folder, "last_results.txt"), "w") as f:
        f.write(last_results)
except:
    print(f'Error trying to write into a new file in output folder "{output_folder}"')
