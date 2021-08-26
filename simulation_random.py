import simpy
from simpy.core import Environment
from trace import Trace
from storage import StorageManager
from policies.policy import Policy
from tqdm import tqdm
import os
import pickle

class Simulation:
    def __init__(self, traces: list[Trace], storage: StorageManager, env: Environment):
        self._env = env
        self._storage = storage
        self._stats = [[0, 0.0, 0, 0.0] for _ in storage.tiers] # n_write, t_write, n_reads, t_reads

        # Adding traces to env as processes
        for trace in traces:
            self._env.process(self._read_trace(trace))

    def run(self):
        self._env.run()
        print("Simulation end! Printing results:")
        for i in range(len(self._stats)):
            # TODO : les stats qu'on voulait aka le nombre d'operations sur chaque tier
            tier = self._storage.tiers[i]
            tier_occupation = sum([file.size for file in tier.content.values()])
            print(f'Tier "{tier.name}" of size {tier.max_size / (10 ** 9)} Gio'
                  f' ({tier_occupation} octets aka {round(tier_occupation/(10 ** 6), 3)} Mio used)'
                  f': {self._stats[i][0]} write ({round(self._stats[i][1], 6)} seconds), {self._stats[i][2]} reads ('
                  f'{round(self._stats[i][3], 6)} seconds)')

    def register_callback(self, event_name, callback):
        pass

    def fire_event(self, event_name, value):
        pass

    def _read_trace(self, trace: Trace):
        last_ts = 0
        # read a line
        with tqdm(total=len(trace.data)) as pbar:
            for line in trace.data:
                pbar.update(1)
                #tstart = line[2]
                tstart = line[1] 
                yield self._env.timeout(max(0, tstart - last_ts)) # traces are sorted by tstart order.
                last_ts = tstart
                self._read_line(line)

    def _read_line(self, line):
        # path, rank, tstart, tend, offset, count, is_read, segments = line
        file_id, tstart, class_size, return_size = line
        path = str(file_id)

        # yield tstart
        # Lock resources (if necessary)

        # Updating the storage. It will create a new file if it's the 1st time we see this path
        file = self._storage.get_file(path)
        time_taken = 0. # Time taken by this io, computed by the tier class
        is_read = True
        if file is None:
            tier = self._storage.get_default_tier()
            time_taken += tier.create_file(tstart, path, class_size)
            is_read = False
        else:
            tier = file.tier
            time_taken += [tier.write_file, tier.read_file][is_read](tstart, path)

        # incrementing stats
        tier_id = self._storage.tiers.index(tier)
        self._stats[tier_id][int(is_read) * 2] += 1
        #self._stats[tier_id][int(is_read) * 2 + 1] += tend - tstart

        # yield tend
        # Unlock resources (not necessary either?)


if __name__ == "__main__":
    from traces import PARADIS_HDF5, TENCENT_DATASET_FILE_THREAD1
    from storage import Tier
    from policies.random_policy import RandomPolicy
    from policies.lifetime_overun_policy import LRU_LifetimeOverunPolicy
    import sys

    log_file = "logs/last_run.txt"
    with open(log_file, 'w') as output:
        print(f'sys.stdout redirected to "./{log_file}".')
        backup_stdout = sys.stdout
        #sys.stdout = output
        env = simpy.Environment()
        if os.path.exists(TENCENT_DATASET_FILE_THREAD1 + '.pickle') :
            with open(TENCENT_DATASET_FILE_THREAD1 + '.pickle', 'rb') as f:
                traces = [pickle.load(f)]
        else:
            traces = [Trace(TENCENT_DATASET_FILE_THREAD1)]
            print('loading with pickle')
            with open(TENCENT_DATASET_FILE_THREAD1 + '.pickle', 'wb') as f:
                pickle.dump(traces[0], f)
            print('file loaded with pickle')

        print('done loading trace')
        tier_ssd = Tier('SSD', 512 * 10 ** 9, 'unknown latency', 'unknown throughput')
        tier_hdd = Tier('HDD', 5 * 10 ** 12, 'unknown latency', 'unknown throughput')
        tier_tapes = Tier('Tapes', 50 * 10 ** 12, 'unknown latency', 'unknown throughput')
        storage = StorageManager([tier_ssd, tier_hdd, tier_tapes], env)
        policy_tier_sdd = RandomPolicy(tier_ssd, storage, env)
        policy_tier_hdd = RandomPolicy(tier_hdd, storage, env)
        sim = Simulation(traces, storage, env)
        sim.run()
        sys.stdout = backup_stdout
        print(f'Done! Check the above-mentioned log file for more details.')