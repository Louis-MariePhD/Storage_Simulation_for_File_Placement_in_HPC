# Storage Simulation for file placement policy evaluation in a heterogeneous multi-tier HPC storage system

This project requires SimPy. It was tested using python 3.8. You can run this project in a Python Virtual Environment using these commands:

```
python -m venv ./venv
source venv/bin/activate
pip install -U -r requirements.txt
```

To evaluate a file placement policy, you must first create a class inheriting the abstract "Policy" class.

Then, you must add it to the command line options by adding it to the "available_policies" dictionnary in \_\_main\_\_.py.

At last, you must either create your own trace parcer class (and add it to the "available_traces" dictionnary in \_\_main\_\_.py), or download from SNIA IOTTA website the missing trace files in the "resources" directory and use the trace parser we already implemented.

When everything is done, run ```python __main__.py --help``` to start interacting with the simulator.
