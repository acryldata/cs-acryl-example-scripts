# Deletes all forms & structured properties in an instance

import logging
from typing import Optional, Iterable
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)

logger = logging.getLogger(__name__)

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

dry_run: bool = True # switch this!

# Utility method to list all assets of a given entity using OpenAPI which is a typed API.
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

form_results = scrollEntity(client, "form", search_body)

for form in form_results:
    form_urn = form['urn']
    references_count, _ = client.delete_references_to_urn(form_urn, dry_run)
    logger.warning(f"{form_urn}, deleted {references_count} references")
    if not dry_run:
        client.delete_entity(form_urn, hard=True)


structured_properties_results = scrollEntity(client, "structuredProperty", search_body)

for structured_prop in structured_properties_results:
    structured_prop_urn = structured_prop['urn'] 
    references_count, _ = client.delete_references_to_urn(structured_prop_urn, dry_run)
    logger.warning(f"{structured_prop_urn}, deleted {references_count} references")
    if not dry_run:
        client.delete_entity(structured_prop_urn, hard=True) 