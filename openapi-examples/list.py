import os
import logging
import json
from typing import Optional, Iterable
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph

logger = logging.getLogger(__name__)

# Utility to get path of executed script regardless of where it is executed from
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()


def scrollEntity(client: DataHubGraph, entity: str, variables: dict) -> Iterable:  # type: ignore
    endpoint = f"{client.config.server}/openapi/v3/entity/{entity}"

    first_iter = True
    scroll_id: Optional[str] = None
    while first_iter or scroll_id:
        first_iter = False
        variables["scrollId"] = scroll_id

        response: dict = client._get_generic(endpoint, variables)

        scroll_id = response.get("scrollId", None)
        for entity in response.get("entities", []):
            yield entity

        logger.debug(f"Scrolling to next page: {scroll_id}")


search_body = {"query": "*", "sort": "urn"}

results = scrollEntity(client, "datahubpolicy", search_body)

print(json.dumps(list(results)))
