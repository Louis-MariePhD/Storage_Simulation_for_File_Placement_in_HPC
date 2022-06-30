#import recorder_viz
import os
import sys
import random
from tqdm import tqdm
from resources import IBM_OBJECT_STORE_FILES
from traces.trace import Trace

_DEBUG = False


class AugmentedIBMObjectStoreTrace(Trace):

    # Column names extracted from recorder_viz, kept here as static members vars
    _COLUMN_NAMES = ("path", "rank", "tstart", "tend",
                     "offset", "count", "isRead", "segments")

    def __init__(self):
        Trace.__init__(self)
        self.file_ids_occurences = {}
        self.lifetime_per_fileid = {}

    def gen_data(self, trace_len_limit=-1, ignore_head=False):
        """
        :return: The trace data as a AoS
        """
        self.data = []
        self.line_count = 0
        self.unique_files = 0
        iteration_count = 0
        last_timestamp = 0

        if trace_len_limit>0:
            sys.stdout.flush()
            pbar = tqdm(total=trace_len_limit, desc="Parsing..."+str(trace_len_limit))

        for path in IBM_OBJECT_STORE_FILES:
            if not trace_len_limit>0:
                sys.stdout.flush()
                pbar = tqdm(total=os.path.getsize(path), desc=f'Parsing trace file {trace}')
            if self.line_count>=trace_len_limit and trace_len_limit>0:
                print(f'Skipping file {path} as we reached the line limit')
                sys.stdout.flush()
                continue
            else:
                print(f'Parsing file {path}')
                sys.stdout.flush()

            with open(path) as f:
                for line in f:
                    if len(line)==0 or self.line_count>=trace_len_limit and trace_len_limit>0:
                        break

                    iteration_count += 1

                    split = line.split(' ')
                    try:
                        timestamp, op_code, uid, size, offset_start, offset_end = (split+[0, 0, 0])[:6]
                        timestamp = int(timestamp)
                    except:
                        continue

                    #assert timestamp >= last_timestamp
                    #last_timestamp = timestamp

                    op_code = op_code.split('.')[1]
                    size = int(size)
                    offset_start = int(offset_start)
                    offset_end = int(offset_end)

                    if ignore_head and op_code == "HEAD":
                        continue

                    if uid not in self.file_ids_occurences.keys():
                        self.unique_files += 1
                        if op_code != "PUT":
                            self.file_ids_occurences[uid] = [1, timestamp, timestamp]
                            self.data += [(timestamp, "PUT", uid, size, offset_start, offset_end)]

                            self.line_count += 1
                            if trace_len_limit > 0:
                                pbar.update(1)

                            if self.line_count>=trace_len_limit:
                                continue
                        else:
                            self.file_ids_occurences[uid] = [0, timestamp, timestamp]

                    self.file_ids_occurences[uid][0] += 1
                    self.file_ids_occurences[uid][2] = max(self.file_ids_occurences[uid][2], timestamp)
                    self.data += [(timestamp, op_code, uid, size, offset_start, offset_end)]
                    pbar.update([len(line), 1][trace_len_limit>0])
                    self.line_count+=1

                    if self.line_count>1000:
                        while random.random() > 0.2:
                            # old_entry = self.data[max(0, len(self.data)-10000):][random.randint(0, min(10000, len(self.data))-1)]
                            old_entry = self.data[random.randint(0, len(self.data)-1)]
                            new_entry = [v for v in old_entry]
                            new_entry[0] = timestamp
                            self.data += [new_entry]
                            self.file_ids_occurences[new_entry[2]][0] += 1
                            self.file_ids_occurences[new_entry[2]][2] = max(self.file_ids_occurences[new_entry[2]][2],
                                                                            timestamp)

                            self.line_count += 1
                            if 0 < trace_len_limit <= self.line_count:
                                break
                                # python __main__.py -t augmented-ibm -l 10000 -i 0.0 lru fifo lifetime && python __main__.py -t augmented-ibm -i 0.5 lifetime -l 10000

            if not trace_len_limit>0:
                pbar.close()
        if trace_len_limit > 0:
            pbar.close()

        sys.stdout.flush()
        print("\nGenerating lifetimes...")
        sys.stdout.flush()

        for k in tqdm(self.file_ids_occurences.keys(), total=self.unique_files, desc="Generating file lifetimes..."):
            v = self.file_ids_occurences[k]
            # v[0] is the number of accesse : unused
            if len(v) > 2:
                creation_time = v[1]
                last_access = v[-1]
                self.lifetime_per_fileid[k] = last_access - creation_time
            else:
                self.lifetime_per_fileid[k] = 0

        sys.stdout.flush()
        print("\nDone loading trace.")
        sys.stdout.flush()

        #print(f'Done loading trace. {round((iteration_count-self.line_count)/iteration_count*100,2)}% '
        #      f'({self.line_count-iteration_count}/{iteration_count}) '
        #      'of the parsed lines had references to files not created in this trace.')

    def read_data_line(self, env, storage, line, simulate_perfect_prefetch: bool = False, logs_enabled = True):
        """Read a line, and fire events if necessary"""

        timestamp, op_code, uid, size, offset_start, offset_end = line

        file = storage.get_file(uid)
        if file is not None: # ignoring files that have not been created
            assert file.path in file.tier.content.keys()
            if simulate_perfect_prefetch and file.tier != storage.get_default_tier():
                assert file.tier != storage.get_default_tier()
                # First move the file to the efficient tier, then do the access
                if logs_enabled:
                    print(f'Prefetching file from tiers {file.tier.name} to {storage.get_default_tier()}')
                storage.migrate(file, storage.get_default_tier(), env.now)
                assert file.path in storage.get_default_tier().content.keys()
            tier = file.tier
        else:
            tier = storage.get_default_tier()

        if file is not None:
            if op_code == "GET":
                tier.read_file(timestamp, uid)
            elif op_code == "HEAD":
                tier.read_file(timestamp, uid)
            elif op_code == "DELETE":
                tier.delete_file(timestamp, uid)
            elif op_code == "COPY":
                print(f'Skipping undefined operation "{(" ".join([str(i) for i in line]))}".')
            elif op_code == "PUT":
                print(f'Invalid use of operation code {op_code} in operation "{(" ".join([str(i) for i in line]))}" '
                      '- file already exist.')
            else:
                raise RuntimeError(f'Unknown operation code {op_code}')
        else:
            if op_code == "PUT":
                tier.create_file(timestamp, uid, size)
                tier.write_file(timestamp, uid)

            elif op_code in ["GET", "HEAD", "DELETE"]:
                raise RuntimeError(f'Invalid use of operation code {op_code} - file does not exist')
            else:
                raise RuntimeError(f'Unknown operation code {op_code}')


    def get_columns_label(self):
        """
        :return: The columns corresponding to the data
        """
        return AugmentedIBMObjectStoreTrace._COLUMN_NAMES

    def timestamp_from_line(self, line):
        return line[0]


if __name__ == "__main__":
    import numpy as np
    import matplotlib.pyplot as plt
    from math import log10
    import math

    len_limit = 5e7
    time_to_seconds = 1e3

    trace = IBMObjectStoreTrace()
    print("Reading trace...")
    trace.gen_data(trace_len_limit=len_limit)

    print(f'Found {len(trace.data)} I/Os for {len(trace.file_ids_occurences.keys())} unique files.')
    p = round(np.sum([1 for i in trace.file_ids_occurences.values()
                      if i[-1]-i[1]>60*time_to_seconds])/len(trace.file_ids_occurences.values())*100.0, 3)
    print(f'%reused 1 min after creation: {p}%')

    #p = trace.out_of_trace_ios/(len(trace.data)+trace.out_of_trace_ios)
    #p = round(p*100.,3)
    #print(f'%of io on files created out of the trace: {p}')

    #p = len(trace.out_of_trace_file_ios.keys())/(len(trace.file_ids_occurences.keys())+len(trace.out_of_trace_file_ios.keys()))
    #p = round(p*100.,3)
    #print(f'%of files created out of the trace: {p}')

    t=trace.timestamp_from_line(trace.data[-1])-trace.timestamp_from_line(trace.data[0])
    t/=time_to_seconds
    print(f'duration of the dataset: {math.floor(t/3600)} hours {math.floor((t%3600)/60)} min {round(t%60,3)} sec')

    trace = IBMObjectStoreTrace()
    print("Now doing the same, but ignoring HEAD commands. Reading trace...")
    trace.gen_data(trace_len_limit=len_limit,ignore_head=True)

    print(f'Found {len(trace.data)} I/Os for {len(trace.file_ids_occurences.keys())} unique files.')
    p = round(np.sum([1 for i in trace.file_ids_occurences.values()
                      if i[-1]-i[1]>60*time_to_seconds])/len(trace.file_ids_occurences.values())*100.0, 3)
    print(f'%reused 1 min after creation: {p}%')

    #p = trace.out_of_trace_ios/(len(trace.data)+trace.out_of_trace_ios)
    #p = round(p*100.,3)
    #print(f'%of io on files created out of the trace: {p}')

    #p = len(trace.out_of_trace_file_ios)/(len(trace.file_ids_occurences.keys())+len(trace.out_of_trace_file_ios))
    #p = round(p*100.,3)
    #print(f'%of files created out of the trace: {p}')

    t=trace.timestamp_from_line(trace.data[-1])-trace.timestamp_from_line(trace.data[0])
    t/=time_to_seconds
    print(f'duration of the dataset: {math.floor(t/3600)} hours {math.floor((t%3600)/60)} min {round(t%60,3)} sec')

    lifetimes = sorted([i[-1]-i[1] for i in trace.file_ids_occurences.values()])
    y = [i/len(trace.file_ids_occurences.values()) for i in range(len(trace.file_ids_occurences.values()))]
    xticks = {"0s":0, "1s":1*time_to_seconds, "10s":10*time_to_seconds, "1 min":60*time_to_seconds, "1h":60*60*time_to_seconds,
              "1 day":60*60*24*time_to_seconds, "1 week":7*60*60*24*time_to_seconds}

    def scale(array):
        return [log10(1+v) for v in array]

    plt.figure()
    plt.plot(scale(lifetimes), y, '-')
    plt.xticks(scale(xticks.values()), xticks.keys())
    plt.grid("off", axis="x")
    plt.show()
