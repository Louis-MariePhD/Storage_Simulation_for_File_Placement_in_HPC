#import recorder_viz
import os
import sys
import datetime
from tqdm import tqdm

_DEBUG = False


class CustomTrace:

    # Column names extracted from recorder_viz, kept here as static members vars
    _COLUMN_NAMES = ("path", "rank", "tstart", "tend",
                     "offset", "count", "isRead", "segments")

    FILE_SIZE_DISTRIBUTION = {
        'l': (33136, 0.1),
        'a': (3263749, 0.5),
        'o': (4925317, 0.7),
        'm': (6043467, 0.85),
        'c': (6050183, 0.95),
        'b': (8387821, 1)}

    def __init__(self, trace_path: str, average_lifetime=None):
        self.data = []

    def gen_data(self, trace_len_limit=-1):
        """
        :return: The trace data as a AoS
        """
        return self.data

    def read_data_line(self, env, storage, line, simulate_perfect_prefetch: bool = False, logs_enabled = True):
        raise NotImplementedError("Bruh.")
