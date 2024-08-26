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

def trigger(client: DataHubGraph, urn: str):
    with open(os.path.join(__location__, "createIngestionExecutionRequest.graphql")) as f:
        query = f.read()

        graphql_query: str = textwrap.dedent(query)
        variables: dict = {"input": 
            {
                "ingestionSourceUrn": urn, 
            }
        }

        response = client.execute_graphql(graphql_query, variables)
        return response.get("createIngestionExecutionRequest", {})
    return 0

# Connect to the DataHub instance configured in your ~/.datahubenv file.
datahub: DataHubGraph = get_default_graph()

# TODO Update this
ingestion_urn="urn:li:dataHubIngestionSource:acfb6a2a-e45a-4070-8b4e-ed95f6aca2f1"

trigger(datahub, ingestion_urn)