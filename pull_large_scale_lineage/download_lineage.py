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

def traverseGraph(server: DataHubGraph, urn: str, direction: str):
    queue = deque()
    root = {"urn": urn, "level": 0}
    queue.append(root)
    seen_entities: dict = {}
    seen_entities[urn] = 0
    while queue:
        node = queue.popleft()
        lineage = graphql_scroll_across_lineage(server, node["urn"], direction)
        for row in lineage:
            if row["urn"] not in seen_entities:
                seen_entities[row["urn"]] = seen_entities[node["urn"]] + 1
                print('{"urn": "' + str(row['urn'])+ '", "level": "' + str(seen_entities[node["urn"]] + 1) + '"}')
                queue.append({"urn": row["urn"], "level": node['level'] + 1})

root_urn=""
(url, token) = get_url_and_token()
parsed_url = urlparse(url)
datahub_server = DataHubGraph(DatahubClientConfig(server=url, token=token))
traverseGraph(datahub_server, root_urn, "UPSTREAM")