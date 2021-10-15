#import recorder_viz
import os
import sys
import datetime

from tqdm import tqdm

from traces.trace import Trace

_DEBUG = False


class SNIATrace(Trace):

    # Column names extracted from recorder_viz, kept here as static members vars
    _COLUMN_NAMES = ("path", "rank", "tstart", "tend",
                     "offset", "count", "isRead", "segments")

    _CHAR2SIZE = {
        'l': 33136,
        'a': 3263749,
        'o': 4925317,
        'm': 6043467,
        'c': 6050183,
        'b': 8387821}

    def __init__(self, trace_path: str):
        Trace.__init__(self)
        self.data = []
        self.file_ids_occurences = {}
        self.lifetime_per_fileid = {}
        self.trace_path = trace_path

    def gen_data(self, trace_len_limit=-1):
        """
        :return: The trace data as a AoS
        """
        with open(self.trace_path) as f:
            print(
                f'[trace-reader] Started loading traces from data folder "{self.trace_path}" into memory')
            line = f.readline()
            line_count = 0
            with tqdm(total=os.path.getsize(self.trace_path)) as pbar:
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
                    class_size = Trace._CHAR2SIZE[columns[3]]  # size of the file (approximation)
                    # number of bytes returned by the request
                    return_size = columns[4]
                    self.data += [[file_id, timestamp, class_size, return_size]]

                    line_count += 1
                    if trace_len_limit > 0 and line_count > trace_len_limit:
                        break

                    line = f.readline()
            reused_percent = round(len([1 for oc in self.file_ids_occurences.values() if oc[0] > 1])
                                   / float(len(self.file_ids_occurences.values())) * 100., 3)
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

    def read_data_line(self, env, storage, line, simulate_perfect_prefetch: bool = False, logs_enabled = True):

        """Read a line, and fire events if necessary"""
        file_id, tstart, class_size, return_size = line
        path = str(file_id)

        # yield tstart
        # Lock resources (if necessary)

        # Updating the storage. It will create a new file if it's the 1st time we see this path
        file = storage.get_file(path)
        time_taken = 0.  # Time taken by this io, computed by the tier class
        is_read = True
        if file is None:
            tier = storage.get_default_tier()
            time_taken += tier.create_file(tstart, path, class_size)
            is_read = False
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
        return Trace._COLUMN_NAMES

    def timestamp_from_line(self, line):
        return line[1]
