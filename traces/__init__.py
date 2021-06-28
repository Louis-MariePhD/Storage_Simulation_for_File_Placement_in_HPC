import os

PATH = os.path.dirname(__file__)
RECORDER2TEXT_PATH = os.path.join(PATH, "recorder2text")

VPIC_PATH_UNI = os.path.join(PATH, "VPIC-IO-0.1/64p/uni/recorder-logs-uni")
VPIC_PATH_NONUNI = os.path.join(PATH, "VPIC-IO-0.1/64p/nonuni/recorder-logs-nonuni")

MILC_QCD_PARALLEL = os.path.join(PATH, "MILC-QCD-7.8.1/64p/clover_dynamical/save_parallel/recorder-logs")

# non-readable trace, corrupted file or recorder-viz having an OutOfMemory error
# MILC_QCD_SERIAL = os.path.join(PATH, "MILC-QCD-7.8.1/64p/clover_dynamical/save_serial/recorder-logs")

PARADIS_HDF5 = os.path.join(PATH, "ParaDis.v2.5.1.1/64p/Copper_HDF5/recorder-logs")
PARADIS_POSIX = os.path.join(PATH, "ParaDis.v2.5.1.1/64p/Copper_POSIX/recorder-logs")

DATASETS_PATHS = [VPIC_PATH_UNI, VPIC_PATH_NONUNI, MILC_QCD_PARALLEL, PARADIS_HDF5, PARADIS_POSIX]
DATASETS_NAMES = ["VPIC (UNI)", "VPIC (NONUNI)", "MILC QCD (PARALLEL)","PARADIS_HDF5", "PARADIS_POSIX"]