#import recorder_viz
import os
import sys
import datetime
from tqdm import tqdm
from resources import IBM_OBJECT_STORE_FILES

_DEBUG = False


class IBMObjectStoreTrace:

    # Column names extracted from recorder_viz, kept here as static members vars
    _COLUMN_NAMES = ("path", "rank", "tstart", "tend",
                     "offset", "count", "isRead", "segments")

    def __init__(self):
        self.file_ids_occurences = {}
        self.lifetime_per_fileid = {}

    def gen_data(self, trace_len_limit=-1):
        """
        :return: The trace data as a AoS
        """
        self.data = []
        self.line_count = 0

        if trace_len_limit>0:
            pbar = tqdm(total=trace_len_limit)

        for path in IBM_OBJECT_STORE_FILES:
            if not trace_len_limit>0:
                pbar = tqdm(total=os.path.getsize(path))
            with open(path) as f:
                line = f.readline()
                while len(line)!=0 and (self.line_count<trace_len_limit or trace_len_limit<=0):
                    pbar.update([len(line), 1][trace_len_limit>0])
                    split = line.split(' ')
                    timestamp, op_code, uid = split[:3]
                    timestamp = int(timestamp)
                    if uid not in self.file_ids_occurences:
                        self.file_ids_occurences[uid] = [1, timestamp]
                    else:
                        self.file_ids_occurences[uid][0]+=1
                        self.file_ids_occurences[uid]+=[timestamp]
                    self.data += [(timestamp, op_code, uid)]
                    self.line_count+=1
                    line = f.readline()
            if not trace_len_limit>0:
                pbar.close()
        if trace_len_limit>0:
            pbar.close()

    def read_data_line(self, env, storage, line, simulate_perfect_prefetch: bool = False, logs_enabled = True):
        """Read a line, and fire events if necessary"""

        file_id, tstart, class_size, return_size = line
        path = str(file_id)

        # Updating the storage. It will create a new file if it's the 1st time we see this path
        file = storage.get_file(path)
        if file is None:
            tier = storage.get_default_tier()
            tier.create_file(tstart, path, class_size)
        else:
            assert file.path in file.tier.content.keys()
            if simulate_perfect_prefetch and file.tier != storage.get_default_tier():
                assert file.tier != storage.get_default_tier()

                # First move the file to the efficient tier, then do the access
                if logs_enabled:
                    print(f'Prefetching file from tiers {file.tier.name} to {storage.get_default_tier()}')

                storage.migrate(file, storage.get_default_tier(), env.now)

                assert file.path in storage.get_default_tier().content.keys()

                file = storage.get_default_tier().content[file.path]
            tier = file.tier
            time_taken += [tier.write_file, tier.read_file][is_read](tstart, path)

    def get_columns_label(self):
        """
        :return: The columns corresponding to the data
        """
        return IBMObjectStoreTrace._COLUMN_NAMES


if __name__ == "__main__":
    import numpy as np
    import matplotlib.pyplot as plt
    from math import log10

    trace = IBMObjectStoreTrace()
    print("Reading trace...")
    trace.gen_data(trace_len_limit=-1)
    p = round(np.sum([1 for i in trace.file_ids_occurences.values()
                      if i[0]>1])/len(trace.file_ids_occurences.values())*100.0, 3)
    print(f'%reused: {p}%')

    lifetimes = sorted([i[-1]-i[1] for i in trace.file_ids_occurences.values()])
    y = [i/len(trace.file_ids_occurences.values()) for i in range(len(trace.file_ids_occurences.values()))]
    xticks = {"0s":0, "1s":1*1000, "10s":10*1000, "1 min":60*1000, "1h":60*60*1000,
              "1 day":60*60*24*1000, "1 week":7*60*60*24*1000}

    def scale(array):
        return [log10(1+v) for v in array]

    plt.figure()
    plt.plot(scale(lifetimes), y, '-')
    plt.xticks(scale(xticks.values()), xticks.keys())
    plt.grid("off", axis="x")
    plt.show()
