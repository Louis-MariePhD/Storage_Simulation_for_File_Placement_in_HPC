#import recorder_viz
import os
import sys
import datetime
from tqdm import tqdm

_DEBUG = False


class Trace:

    # Column names extracted from recorder_viz, kept here as static members vars
    _COLUMN_NAMES = ("path", "rank", "tstart", "tend",
                     "offset", "count", "isRead", "segments")

    def gen_data(self, trace_len_limit=-1):
        """
        :return: The trace data as a AoS
        """
        raise NotImplementedError("Using unspecialized trace class.")

    def read_data_line(self, env, storage, line, simulate_perfect_prefetch: bool = False, logs_enabled = True):
        raise NotImplementedError("Using unspecialized trace class.")

    def get_columns_label(self):
        """
        :return: The columns corresponding to the data
        """
        return Trace._COLUMN_NAMES
