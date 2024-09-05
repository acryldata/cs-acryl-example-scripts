import os
import logging
import textwrap
from typing import List
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)
from datahub.ingestion.graph.filters import SearchFilterRule

logger = logging.getLogger(__name__)

# Utility to get path of executed script regardless of where it is executed from
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

def subscribe_to(urn: str, actors: list[str]):
    with open(os.path.join(__location__, "createSubscription.graphql")) as f:
        query = f.read()

        graphql_query: str = textwrap.dedent(query)
        variables: dict = {"input": 
            {
                "entityUrn": urn, 
                "subscriptionTypes": "", # TODO update this based on the subscription you want to create
                "entityChangeTypes": "", # TODO update this based on the subscription you want to create
            }
        }

        response = client.execute_graphql(graphql_query, variables)
        return response.get("listIngestionSources", {})

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

# TODO Update this
term_urn="urn:li:glossaryTerm:ef5085ba63d081d70cbfbeebc2795374"

# TODO Update this
actor_urns: List[str] = ["urn:li:corpuser:admin"]

# Filter to pull subscriptions that have a running status
filters = []
filter_criteria: List[SearchFilterRule] = [
    {
        "field": "glossaryTerms",
        "condition": "EQUAL",
        "values": [term_urn],
    }
]

urns_to_subscribe_to = client.get_urns_by_filter(
    query="*",
    extraFilters=filters
)

for urn in urns_to_subscribe_to:
    subscribe_to(urn, actor_urns)