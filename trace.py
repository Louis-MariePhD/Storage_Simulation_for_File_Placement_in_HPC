import recorder_viz
import os
import sys

_DEBUG = False

class Trace:

    # Column names extracted from recorder_viz, kept here as static members vars
    _COLUMN_NAMES = ("path", "rank", "tstart", "tend", "offset", "count", "isRead", "segments")

    def __init__(self, trace_path, trace_name="Unnamed trace folder"):
        """
        Load a trace from the given path, then format it to a readable & iterable format
        :param trace_path: eg. /traces/VPIC-IO-0.1/64p/uni/recorder-logs-uni
        """

        # can't disable the extremely verbose prints in recorder_viz. So we mute stdout for a while.
        if not _DEBUG:
            default_stdout = sys.stdout
            dev_null = open(os.devnull, 'w')
            sys.stdout = dev_null

        # Loading dataset
        reader = recorder_viz.RecorderReader(trace_path)
        intervals = recorder_viz.reporter.build_offset_intervals(reader)

        # From dict of tuple to array of tuple for easier sorting. AoS probably better than SoA in our case.
        self.data = []
        file_count = 0
        print(f'[trace-reader] Started loading traces from data folder "{trace_path}" into memory')
        for path in intervals.keys():
            file_count += 1
            print(f'[trace-reader] Reading traces for file {path} ({file_count}/{len(intervals.keys())})')
            for line in intervals[path]:
                self.data += [[path, *line]]

        # Makes it easier to position multiple traces in a simulation
        # if timestamp_offset != 0:
        #    for line in self.data:
        #        line[2] += timestamp_offset
        #        line[3] += timestamp_offset

        print(f'[trace-reader] Sorting data based on timestamp...')
        self.data.sort(key=lambda _line: _line[2])  # 2 column index of tstart. Magic value for the sake of performance.

        if not _DEBUG: # Same as the above, we restore the standard stdout
            sys.stdout = default_stdout
            dev_null.close()

        print(f'[trace-reader] Done loading trace "{trace_name}", for a total of {len(self.data)} '
              f'read/writes operations, on {file_count} uniques file names, '
              f'over a duration of {round(self.data[-1][2]-self.data[0][2], 6)} sec.')

    def from_metadata_snapshot(self, snapshot):
        pass

    def get_data(self):
        """
        :return: The trace data as a AoS
        """
        return self.data

    @staticmethod
    def get_columns_label():
        """
        :return: The columns corresponding to the data
        """
        return Trace._COLUMN_NAMES


if __name__ == "__main__":
    from traces import VPIC_PATH_UNI
    _DEBUG = True
    trace = Trace(VPIC_PATH_UNI)
    print(trace.get_data()[:10])
