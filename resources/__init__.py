import os

PATH = os.path.dirname(__file__)

TENCENT_DATASET_FILE_THREAD1 = os.path.join(PATH, "dataset_tencent/http_thread1_normal.log.17-24")

IBM_OBJECT_STORE_FILES = [f'{PATH}/dataset_ibm/{path}' for path in os.listdir(os.path.join(PATH, "dataset_ibm"))
                          if path.split('.')[-1] not in ["tgz", "sh", "bat"]]