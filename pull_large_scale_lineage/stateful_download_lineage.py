# WARNING: THIS IS A SLOW IMPLEMENTATION BECAUSE WE HAVE TO LOAD THE GRAPH INTO MEMORY 
# AND THEN RE-COMPUTE THE QUEUE STATE.
#
# The output of this file is a file that can be used as a state file for subsequent executions:
# python stateful_download_lineage.py > state.json
#
# The idea is that you recursively call the function and it will "just work"

# Possible improvements
# - External DB for in-memory graph representation
# - Re-think resumeable logic (having to linearly pop the queue & delete state is not ideal)
# - Use a different (more efficient?) graphQL endpoint, such as the `entities` call just asking for relationships
# - Bulk queue pops of the same level to reduce calls to GMS (the previous mentioned GraphQL call can take a list of urns)
# - Other things?

import json
import textwrap
from collections import deque
from urllib.parse import urlparse
from typing import Optional
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig, get_url_and_token

def graphql_scroll_across_lineage(graph: DataHubGraph, entity_urn: str, direction: str):
    with open("scrollAcrossLineage.graphql") as f:
        query = f.read()
        
        graphql_query = textwrap.dedent(query)

        variables = {
            "input": {
                "urn": entity_urn,
                "direction": direction,
                "query": "",
                "count": 5000, # pull 5000 results each time at most
                "orFilters": [{
                    "and": [{
                        "field": "degree",
                        "condition": "EQUAL",
                        "values": ["1"],
                        "negated": "false"
                        }]
                }]
            }
        }

        first_iter = True
        scroll_id: Optional[str] = None
        while first_iter or scroll_id:
            first_iter = False
            variables["input"]["scrollId"] = scroll_id
            response = graph.execute_graphql(
                graphql_query,
                variables=variables,
            )
            data = response["scrollAcrossLineage"]
            scroll_id = data["nextScrollId"]
            for entry in data["searchResults"]:
                yield entry["entity"]

def traverseGraph(server: DataHubGraph, direction: str, queue: deque = deque(), seen_entities: dict = {}):
    while queue:
        node = queue.popleft()
        print(json.dumps(node))
        lineage = graphql_scroll_across_lineage(server, node["urn"], direction)
        for row in lineage:
            if row["urn"] not in seen_entities:
                obj = {"urn": row["urn"], "parent": node["urn"], "level": int(node["level"]) + 1}
                seen_entities[row["urn"]] = {"parent": obj["parent"], "level": obj["level"]}
                queue.append(obj)

root_urn=""
state_file="state.json"
root_node = {"urn": root_urn, "parent": "", "level": 0}

queue = deque()
state: dict = {}

with open(f"./{state_file}", 'r') as f:
    last_level = 0
    for line in f:
        obj = json.loads(line)
        state[obj["urn"]] = {"parent": obj["parent"], "level": obj["level"]}
        queue.append(obj)
        last_level=obj["level"]

    if not queue: # if queue empty
        state = root_node
        queue.append(root_node)
    else:
        # remove last level which may be incomplete
        while queue[-1]["level"] == last_level:
            obj = queue.pop()
            del state[obj["urn"]]
        # remove all levels until the last one we need to re-process
        while queue[0]["level"] < last_level - 1:
            print(json.dumps(queue.popleft()))

(url, token) = get_url_and_token()
parsed_url = urlparse(url)
datahub_server = DataHubGraph(DatahubClientConfig(server=url, token=token))
traverseGraph(datahub_server, "UPSTREAM", queue, state)