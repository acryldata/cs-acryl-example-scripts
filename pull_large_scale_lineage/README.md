This will pull lineage iteratively one hop at a time. 

There is a version that does NOT save state and a version that does, as indicated by the script names.



### Non-stateful version

For the download_lineage.py version:

You will need to:
    - Use python 3.10+,
    - Install acryl-datahub pip package
    - Have your DataHub credentials configured in ~/.datahubenv
Info:
    - The script will emit to standard output json objects in every line
    - This script can not be stopped, otherwise it will have to traverse the graph from scratch and re-compute everything.
    - Note that this live queries DataHub which means that if graph updates are pushed to DataHub, this will pick up whatever was in the database at the time of querying.

To run the script:
    python download_lineage.py > upstreams.json

### Stateful version

For the stateful_download_lineage.py version:

You will need to:
    - Use python 3.10+,
    - Install acryl-datahub pip package
    - Have your DataHub credentials configured in ~/.datahubenv

The output of this file is a file that can be used as a state file for subsequent executions:
python stateful_download_lineage.py > state.json