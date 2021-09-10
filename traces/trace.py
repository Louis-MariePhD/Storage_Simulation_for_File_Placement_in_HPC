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

    def get_data(self):
        """
        :return: The trace data as a AoS
        """
        raise NotImplementedError("Using unspecialized trace class.")

    @staticmethod
    def get_columns_label():
        """
        :return: The columns corresponding to the data
        """
        return Trace._COLUMN_NAMES
