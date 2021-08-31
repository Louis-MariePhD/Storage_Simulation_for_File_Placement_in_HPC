import simpy
from simpy.core import Environment
from trace import Trace
from storage import StorageManager
from tqdm import tqdm
import sys
import os
import pickle


class Simulation:
    def __init__(self, traces: "list[Trace]", storage: StorageManager, env: Environment, log_file = "logs/last_run.txt"):
        self._env = env
        self._storage = storage
        self._stats = [[0, 0.0, 0, 0.0] for _ in storage.tiers]  # n_write, t_write, n_reads, t_reads
        self._log_file = log_file

        # Adding traces to env as processes
        for trace in traces:
            self._env.process(self._read_trace(trace, True))

    def run(self):
        """Start the simpy simulation loop. At the end of the simulation, prints the results"""
        self._env.run()
        print("Simulation end! Printing results:")
        for i in range(len(self._stats)):
            tier = self._storage.tiers[i]
            tier_occupation = sum([file.size for file in tier.content.values()])
            print(f'Tier "{tier.name}" of size {tier.max_size / (10 ** 9)} Gio'
                  f' ({tier_occupation} octets aka {round(tier_occupation / (10 ** 6), 3)} Mio used)'
                  f': {self._stats[i][0]} write ({round(self._stats[i][1], 6)} seconds), {self._stats[i][2]} reads ('
                  f'{round(self._stats[i][3], 6)} seconds)')

    def _read_trace(self, trace: Trace, simulate_perfect_prefetch: bool = False):
        """Read a trace as a line list, while updating a progress bar. Runs self._read_line for each line"""
        last_ts = 0
        with open(self._log_file, 'w') as log:
            backup_stdout = sys.stdout
            with tqdm(total=len(trace.data), file=sys.stdout) as pbar:
                sys.stdout = log
                print(f'sys.stdout redirected to "./{self._log_file}".')
                for line in trace.data:
                    pbar.update(1)
                    # tstart = line[2]
                    tstart = line[1]
                    yield self._env.timeout(max(0, tstart - last_ts))  # traces are sorted by tstart order.
                    last_ts = tstart
                    self._read_line(line)
            sys.stdout = backup_stdout
            print(f'Done! Check the above-mentioned log file for more details.')

    def _read_line(self, line, simulate_perfect_prefetch: bool = False):
        """Read a line, and fire events if necessary"""
        file_id, tstart, class_size, return_size = line
        path = str(file_id)

        # yield tstart
        # Lock resources (if necessary)

        # Updating the storage. It will create a new file if it's the 1st time we see this path
        file = self._storage.get_file(path)
        time_taken = 0.  # Time taken by this io, computed by the tier class
        is_read = True
        if file is None:
            tier = self._storage.get_default_tier()
            time_taken += tier.create_file(tstart, path, class_size)
            is_read = False
        else:
            if simulate_perfect_prefetch and file.tier != self._storage.get_default_tier():
                # First move the file to the efficient tier, then do the access
                self._storage.migrate(file, self._storage.get_default_tier(), self._env.now)
            tier = file.tier
            time_taken += [tier.write_file, tier.read_file][is_read](tstart, path)

        # incrementing stats
        tier_id = self._storage.tiers.index(tier)
        self._stats[tier_id][int(is_read) * 2] += 1
        # self._stats[tier_id][int(is_read) * 2 + 1] += tend - tstart

        # yield tend
        # Unlock resources (not necessary either?)
