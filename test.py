from traces.ibm_object_store_trace import IBMObjectStoreTrace
import numpy as np

if __name__ == "__main__":
    trace = IBMObjectStoreTrace()
    print("Reading trace...")
    trace.gen_data()
    print(f'%reused: {round(np.sum([1 for i in trace.file_ids_occurences if i[0]>1])/trace.line_count*100.0, 3)}%')