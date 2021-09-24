#import recorder_viz
import os
import sys
import datetime
from tqdm import tqdm
from resources import IBM_OBJECT_STORE_FILES
from traces.trace import Trace

_DEBUG = False


class IBMObjectStoreTrace(Trace):

    # Column names extracted from recorder_viz, kept here as static members vars
    _COLUMN_NAMES = ("path", "rank", "tstart", "tend",
                     "offset", "count", "isRead", "segments")

    def __init__(self):
        Trace.__init__(self)
        self.file_ids_occurences = {}
        self.lifetime_per_fileid = {}

    def gen_data(self, trace_len_limit=-1):
        """
        :return: The trace data as a AoS
        """
        self.data = []
        self.line_count = 0
        iteration_count = 0

        if trace_len_limit>0:
            pbar = tqdm(total=trace_len_limit)

        for path in IBM_OBJECT_STORE_FILES:
            if not trace_len_limit>0:
                pbar = tqdm(total=os.path.getsize(path))
            with open(path) as f:
                line = f.readline()
                while len(line)!=0 and (self.line_count<trace_len_limit or trace_len_limit<=0):
                    iteration_count += 1
                    split = line.split(' ')

                    timestamp, op_code, uid, size, offset_start, offset_end = (split+[0, 0, 0])[:6]
                    timestamp = int(timestamp)
                    op_code = op_code.split('.')[1]
                    size = int(size)
                    offset_start = int(offset_start)
                    offset_end = int(offset_end)

                    if uid not in self.file_ids_occurences:
                        if op_code != "PUT":
                            self.data += [(timestamp, "PUT", uid, size, offset_start, offset_end)]
                            pbar.update([0, 1][trace_len_limit > 0])
                            self.line_count += 1
                            if self.line_count>=trace_len_limit and trace_len_limit>0:
                                continue
                        self.file_ids_occurences[uid] = [1, timestamp]
                    self.file_ids_occurences[uid][0]+=1
                    self.file_ids_occurences[uid]+=[timestamp]
                    self.data += [(timestamp, op_code, uid, size, offset_start, offset_end)]
                    pbar.update([len(line), 1][trace_len_limit>0])
                    self.line_count+=1
                    line = f.readline()
            if not trace_len_limit>0:
                pbar.close()
        if trace_len_limit>0:
            pbar.close()
        print(f'Done loading trace. {round((self.line_count-iteration_count)/iteration_count*100,2)}% '
              f'({self.line_count-iteration_count}/{iteration_count}) '
              'of the parsed lines had references to files not created in this trace.')

    def read_data_line(self, env, storage, line, simulate_perfect_prefetch: bool = False, logs_enabled = True):
        """Read a line, and fire events if necessary"""

        timestamp, op_code, uid, size, offset_start, offset_end = line

        file = storage.get_file(uid)
        if file is not None: # ignoring files that has no been created
            assert file.path in file.tier.content.keys()
            if simulate_perfect_prefetch and file.tier != storage.get_default_tier():
                assert file.tier != storage.get_default_tier()
                # First move the file to the efficient tier, then do the access
                if logs_enabled:
                    print(f'Prefetching file from tiers {file.tier.name} to {storage.get_default_tier()}')
                storage.migrate(file, storage.get_default_tier(), env.now)
                assert file.path in storage.get_default_tier().content.keys()

            tier = file.tier
            if op_code == "PUT":
                tier.create_file(timestamp, uid)
            elif op_code == "GET":
                tier.read_file(timestamp, uid)
            elif op_code == "HEAD":
                tier.read_file(timestamp, uid)
            elif op_code == "DELETE":
                tier.delete_file(timestamp, uid)
            else:
                raise RuntimeError(f'Unknown operation code {op_code}')

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
                      if i[-1]-i[1]>10])/len(trace.file_ids_occurences.values())*100.0, 3)
    print(f'%reused 1 min after creation: {p}%')

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
