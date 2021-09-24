import os
import datetime
import random
from trace import Trace

from tqdm import tqdm

from traces.snia_trace import SNIATrace

_DEBUG = False


class AugmentedSNIATrace(SNIATrace):

    # Column names extracted from recorder_viz, kept here as static members vars
    _COLUMN_NAMES = ("path", "rank", "tstart", "tend",
                     "offset", "count", "isRead", "segments")
    _TENCENT_DATASET_COLUMN_NAMES = ("timestamp", "")

    _CHAR2SIZE = {
        'l': 33136,
        'a': 3263749,
        'o': 4925317,
        'm': 6043467,
        'c': 6050183,
        'b': 8387821}

    def __init__(self, trace_path: str):
        super(self, trace_path)

    def gen_data(self, trace_len_limit=-1):
        """
        :return: The trace data as a AoS
        """
        with open(self.trace_path) as f:
            print(
                f'[trace-reader] Started loading traces from data folder "{self.trace_path}" into memory')
            line = f.readline()
            line_count = 0
            with tqdm(total = os.path.getsize(self.trace_path)) as pbar:
                line = f.readline()
                while len(line) != 0:
                    pbar.update(len(line))
                    columns = line.split(' ')
                    timestamp = int(datetime.datetime.strptime(
                        columns[0], "%Y%m%d%H%M%S").timestamp())
                    file_id = columns[1]
                    if file_id not in self.file_ids_occurences:
                        self.file_ids_occurences[file_id] = [1, timestamp]
                    else:
                        self.file_ids_occurences[file_id][0] += 1
                        self.file_ids_occurences[file_id].append(timestamp)
                    class_size = AugmentedSNIATrace._CHAR2SIZE[columns[3]]  # size of the file (approximation)
                    # number of bytes returned by the request
                    return_size = columns[4]
                    self.data += [[file_id, timestamp, class_size, return_size]]

                    line_count+=1
                    if 0 < trace_len_limit < line_count:
                        break

                    if line_count>1000:
                        while random.random() > 0.2:
                            # old_entry = self.data[max(0, len(self.data)-10000):][random.randint(0, min(10000, len(self.data))-1)]
                            old_entry = self.data[random.randint(0, len(self.data)-1)]
                            new_entry = [v for v in old_entry]
                            new_entry[1] = timestamp
                            self.data += [new_entry]
                            self.file_ids_occurences[new_entry[0]][0] += 1
                            self.file_ids_occurences[new_entry[0]].append(timestamp)

                            line_count += 1
                            if 0 < trace_len_limit < line_count:
                                break


                    line = f.readline()
            reused_percent = round(len([1 for oc in self.file_ids_occurences.values() if oc[0] > 1])
                                   / float(len(self.file_ids_occurences.values()))*100.,3)
            print(f'[trace-reader] Done loading trace "{self.trace_path}", for a total of {len(self.data)} '
                  f'read/writes operations, on {len(self.file_ids_occurences)} uniques file names. '
                  f'{reused_percent}% of files are reused after their creation.')

        for k in self.file_ids_occurences.keys():
            v = self.file_ids_occurences[k]
            # v[0] is the number of accesse : unused
            if len(v) > 2:
                creation_time = v[1]
                last_access = v[-1]
                self.lifetime_per_fileid[k] = last_access - creation_time
            else:
                self.lifetime_per_fileid[k] = 0
        return self.data

    def get_columns_label(self):
        """
        :return: The columns corresponding to the data
        """
        return Trace._COLUMN_NAMES
