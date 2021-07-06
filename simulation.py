import simpy


class Simulation:
    def __init__(self, traces, storage, policy, env = None):
        if env is not None:
            self._env = env
        else:
            self._env = simpy.Environment()
        self._policy = policy
        self._storage = storage
        self._stats = [[0, 0.0, 0, 0.0] for _ in storage.tier_sizes] # n_write, t_write, n_reads, t_reads

        # Adding traces to env as processes
        for trace in traces:
            self._env.process(self._read_trace(trace))

    def run(self):
        self._env.run()
        print("Simulation end! Printing results:")
        for i in range(len(self._stats)):
            tier_occupation = sum([metadata[0] for metadata in self._storage.tier_contents_metadata[i]])
            print(f'Tier "{self._storage.tier_names[i]}" of size {self._storage.tier_sizes[i] / (2 ** (10 * 30))} Gio'
                  f' ({tier_occupation} octets / {round(tier_occupation/(2**(10*2)), 3)} Mio used): {self._stats[i][0]}'
                  f' write ({round(self._stats[i][1], 6)} seconds), {self._stats[i][2]} reads ('
                  f'{round(self._stats[i][3], 6)} seconds)')

    def register_callback(self, event_name, callback):
        pass

    def fire_event(self, event_name, value):
        pass

    def _read_trace(self, trace):
        last_ts = 0
        # read a line
        for line in trace.data:
            tstart = line[2]
            yield self._env.timeout(max(0, tstart - last_ts)) # traces are sorted by tstart order.
            last_ts = tstart
            self._read_line(line)

    def _read_line(self, line):
        path, rank, tstart, tend, offset, count, is_read, segments = line
        # yield tstart
        # Lock resources (if necessary)

        # Updating the storage. It will create a new file if it's the 1st time we see this path
        file = [self._storage.sim_write, self._storage.sim_read][is_read](tstart, tend, path, offset, count)

        # incrementing stats
        path, id, tier_id, size, ctime, last_mod, last_access = file
        self._stats[tier_id][int(is_read) * 2] += 1
        self._stats[tier_id][int(is_read) * 2 + 1] += tend - tstart

        # yield tend
        # Unlock resources (not necessary either?)


if __name__ == "__main__":
    from traces import PARADIS_HDF5
    from storage import StorageManager
    from policies.demo_policy import DemoPolicy
    from trace import Trace
    import sys

    with open("logs/last_run.log", 'w') as output:
        sys.stdout = output
        traces = [Trace(PARADIS_HDF5)]
        env = simpy.Environment()
        storage = StorageManager(env, tier_sizes=[256 * 2 ** (10 * 30), 2000 * 2 ** (10 * 30), 10000 * 2 ** (10 * 30)], tier_names=["SSD", "HDD", "Tapes"])
        policy = DemoPolicy(storage, env)
        sim = Simulation(traces, storage, policy, env)
        sim.run()