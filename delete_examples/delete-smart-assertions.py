# Deletes all smart assertions in an instance + associated monitors

import logging
from typing import Optional
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)

logger = logging.getLogger(__name__)

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

dry_run: bool = True # switch this!

scroll_gql = """
query scrollAcrossEntities($input:  ScrollAcrossEntitiesInput!) {
    scrollAcrossEntities(input: $input){
        nextScrollId,
        searchResults {
        entity { 
            ... on Assertion {
            urn
            monitor: relationships(input: {types: ["Evaluates"], direction: INCOMING, start: 0, count: 1}) {
                relationships{
                entity {
                    ... on Monitor {
                            urn
                        }
                }
                } 
            }
            }
        }
    }
  }
}
"""

delete_assertion_gql = """
mutation deleteAssertion($input: String!) {
    deleteAssertion(urn: $input)
}
"""

delete_monitor_gql = """
mutation deleteMonitor($input: String!) {
    deleteMonitor(urn: $input)
}
"""

def scrollGraphQL(client: DataHubGraph, query: str, variables: dict):
    first_iter = True
    scroll_id: Optional[str] = None
    while first_iter or scroll_id:
        first_iter = False
        variables["input"]["scrollId"] = scroll_id
        response = client.execute_graphql(
            query,
            variables=variables,
        )
        data = response["scrollAcrossEntities"]
        scroll_id = data["nextScrollId"]
        for entry in data["searchResults"]:
            yield entry["entity"]


variables = {
    "input": {
        "query":"/q sourceType: INFERRED", 
        "types": ["ASSERTION"],
        "sortInput":{ "sortCriteria": [
            {"field": "urn", "sortOrder": "ASCENDING"}
        ]}
    }
}

# Store results in a list to eagerly pull down all results, otherwise this script is reading and 
# pushing deletes at the same time causing the elasticsearch scroll to be inconsistent
# 
results = list(scrollGraphQL(client, scroll_gql, variables))
print(len(results))

for result in results:
    assertion_urn = result["urn"]
    
    if "monitor" not in result:
        logger.warning(f"{assertion_urn} has no monitor")
        continue

    monitor = result["monitor"]

    if "relationships" not in monitor:
        logger.warning(f"{assertion_urn} has no monitor relationship")
        continue

    relationships = monitor["relationships"]

    if len(relationships) < 1:
        logger.warning(f"{assertion_urn} has an empty relationship list to monitors")
        continue

    for relationship in relationships:
        if "entity" not in relationship:
            logger.warning(f"{assertion_urn} has an empty relationship entry: {relationship}")
            continue
        
        monitor_obj = relationship["entity"]

        if "urn" not in monitor_obj:
            logger.warning(f"{assertion_urn} has a monitor relationship with no urn: {monitor_obj}")
            continue

        monitor_urn = monitor_obj["urn"]

        if dry_run:
            print(f"Deleting assertion {assertion_urn} & monitor: {monitor_urn}")
            continue

        if not dry_run:
            delete_monitor_variables = {
                "input": str(monitor_urn)
            }
            client.execute_graphql(delete_monitor_gql, delete_monitor_variables)

    if not dry_run:
        delete_assertion_variables = {
            "input": str(assertion_urn)
        }
        client.execute_graphql(delete_assertion_gql, delete_assertion_variables)
        