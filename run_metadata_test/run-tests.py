import os
import logging
import textwrap
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Utility to get path of executed script regardless of where it is executed from
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def run_tests_for(urn: str):
    with open(os.path.join(__location__, "runTests.graphql")) as f:
        query = f.read()

        graphql_query: str = textwrap.dedent(query)
        variables: dict = {"urn": urn}

        response = client.execute_graphql(graphql_query, variables)
        return response.get("runTests", {})


def get_search_count(client: DataHubGraph, query_str: str) -> int:
    with open(os.path.join(__location__, "aggregateAcrossEntities.graphql")) as f:
        query = f.read()

        graphql_query: str = textwrap.dedent(query)
        variables: dict = {
            "input": {
                "query": query_str,
                "facets": ["_entityType"]
            }
        }

        response = client.execute_graphql(graphql_query, variables)
        aggregationAcrossEntities = response.get("aggregateAcrossEntities", {})
        facets = aggregationAcrossEntities.get("facets", [])
        if len(facets) == 0:
            return 0
        
        count = sum(agg.get('count', 0) for agg in facets[0]['aggregations'])
        return count


# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

query = "/q hasFailingTests: true OR hasPassingTests: true"

count: int = get_search_count(client, query)
if count == 0:
    logger.warning(f"No datasets found that had failing test executions")
    exit(0)

urns_to_run_tests_for = client.get_urns_by_filter(query=query)

for urn in urns_to_run_tests_for:
    run_tests_for(urn)

# Example code to sanity check that the aggregate count is the same as urns_by_filter.
# This only works if the number of aggregation buckets is less than 20.

#logger.info(f"From aggregate endpoint: {count}")
#import json
#from collections import Counter
#from datahub.utilities.urns.urn import guess_entity_type
#logger.info(f"From scroll endpoint: {json.dumps(dict(Counter(map(guess_entity_type, urns_to_run_tests_for))))}")
#exit(0)
