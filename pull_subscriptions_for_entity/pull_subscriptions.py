import logging
from typing import List, Optional, Dict
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.schema_classes import SubscriptionInfoClass

logger = logging.getLogger(__name__)

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

# TODO Update this
subscribed_urn=""

# Filter to pull subscriptions that have a running status
filters = []
filter_criteria: List[SearchFilterRule] = [
    {
        "field": "entityUrn",
        "condition": "EQUAL",
        "values": [subscribed_urn],
    }
]

filters.append({"and": filter_criteria})
search_body = {
    "input": "*",
    "entity": "subscription",
    "start": 0,
    "count": 10,
    "filter": {"or": filters},
}

results: Dict = client._post_generic(client._search_endpoint, search_body)
num_entities = results.get("value", {}).get("numEntities", 0)
if num_entities == 0:
    logger.warning(
        f"No subscriptions found for {subscribed_urn}"
    )
    exit(0)

urns: List[str] = [entry["entity"] for entry in results["value"]["entities"]]

for urn in urns:
    subscription: Optional[SubscriptionInfoClass] = client.get_aspect(entity_urn=urn, aspect_type=SubscriptionInfoClass)
    if not subscription:
        logger.warning(f"No subscription info found for urn: {urn}")
    print(subscription.to_obj().get("actorUrn"))
