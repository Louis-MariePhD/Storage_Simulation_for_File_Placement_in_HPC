from simpy.core import Environment
from trace import Trace
from storage import StorageManager
from tqdm import tqdm
import sys
import os
import time


class Simulation:
    def __init__(self, traces: "list[Trace]", storage: StorageManager, env: Environment, log_file="logs/last_run.txt",
                 progress_bar_enabled=True, logs_enabled=True):
        self._env = env
        self._storage = storage
        self._log_file = log_file
        self._progress_bar_enabled = progress_bar_enabled
        self._logs_enabled = logs_enabled

        # Adding traces to env as processes
        for trace in traces:
            self._env.process(self._read_trace(trace, True))

    def run(self):
        """Start the simpy simulation loop. At the end of the simulation, prints the results"""
        t0 = time.time()
        self._env.run()
        print(f'Simulation finished after {round(time.time()-t0, 3)} seconds! Printing results:')
        s = f'\n{" "*4}>> '
        s2 = f'\n{" "*8}>> '
        for tier in self._storage.tiers:
            tier_occupation = sum([file.size for file in tier.content.values()])
            total_migration_count = tier.number_of_eviction_from_this_tier+tier.number_of_eviction_to_this_tier+\
            tier.number_of_prefetching_from_this_tier+tier.number_of_prefetching_to_this_tier
            print(f'Tier "{tier.name}":'
                  f'{s}Size {tier.max_size / (10 ** 9)} Go ({tier_occupation} octets)'
                  f'{s}{total_migration_count} migrations'
                  f'{s2}{tier.number_of_prefetching_to_this_tier} due to prefetching to this tiers'
                  f'{s2}{tier.number_of_prefetching_from_this_tier} due to prefetching from this tiers'
                  f'{s2}{tier.number_of_eviction_to_this_tier} due to eviction to this tiers'
                  f'{s2}{tier.number_of_eviction_from_this_tier} due to eviction from this tiers'
                  f'{s}{tier.number_of_write} total write'
                  f'{s2}{tier.number_of_write-tier.number_of_prefetching_to_this_tier-tier.number_of_eviction_to_this_tier} because of user activity '
                  f'{s2}{tier.number_of_prefetching_to_this_tier+tier.number_of_eviction_to_this_tier} '
                  'because of migration'
                  f'{s}{tier.number_of_reads} total reads'
                  f'{s2}{tier.number_of_reads-tier.number_of_prefetching_from_this_tier-tier.number_of_eviction_from_this_tier} because of user activity'
                  f'{s2}{tier.number_of_prefetching_from_this_tier+tier.number_of_eviction_from_this_tier} '
                  'because of migration')

    def _read_trace(self, trace: Trace, simulate_perfect_prefetch: bool = False):
        """Read a trace as a line list, while updating a progress bar. Runs self._read_line for each line"""
        last_ts = 0
        backup_stdout = sys.stdout
        if self._logs_enabled:
            os.makedirs(os.path.dirname(self._log_file), exist_ok=True)
            print(f'sys.stdout redirected to "./{self._log_file}".')
            sys.stdout = open(self._log_file, 'w')
        else:
            sys.stdout = open(os.devnull, "w+")
        if self._progress_bar_enabled:
            pbar = tqdm(total=len(trace.data), file=backup_stdout)
        for line in trace.data:
            if self._progress_bar_enabled:
                pbar.update(1)
            # tstart = line[2]
            tstart = line[1]
            yield self._env.timeout(max(0, tstart - last_ts))  # traces are sorted by tstart order.
            last_ts = tstart
            self._read_line(line, simulate_perfect_prefetch)

        if self._progress_bar_enabled:
            pbar.close()
        log_stream = sys.stdout
        sys.stdout = backup_stdout
        log_stream.close()
        print(f'Done simulating! sys.stdout was restored. Check the log file if enabled for more details on the simulation.')

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
                if self._logs_enabled:
                    print(f'Prefetching file from tiers {file.tier.name} to {self._storage.get_default_tier()}')
                self._storage.migrate(file, self._storage.get_default_tier(), self._env.now)
                file = self._storage.get_default_tier().content[file.path]
            tier = file.tier
            time_taken += [tier.write_file, tier.read_file][is_read](tstart, path)

        # yield tend
        # Unlock resources (not necessary either?)
