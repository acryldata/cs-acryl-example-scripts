### Script to get the list of the private channels that are subscribed to Slack notifications on DataHub
import os
import json
import logging
from typing import Iterable
from datahub.metadata.schema_classes import SubscriptionInfoClass, SubscriptionNotificationConfigClass, SlackNotificationSettingsClass
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# Utility to get path of executed script regardless of where it is executed from
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()


def scrollEntity(client: DataHubGraph, entity: str, variables: dict) -> Iterable:
    endpoint = f"{client.config.server}/openapi/v3/entity/{entity}"

    first_iter = True
    scroll_id: str | None = None
    while first_iter or scroll_id:
        first_iter = False
        variables["scrollId"] = scroll_id

        response: dict = client._get_generic(endpoint, variables)

        scroll_id = response.get("scrollId", None)
        for entity in response.get("entities", []):
            yield entity

        logger.debug(f"Scrolling to next page: {scroll_id}")


search_body = {"query": "*", "sort": "urn"}

results = scrollEntity(client, "subscription", search_body)
unique_channels: set[str] = set()
for result in results:
    logger.debug(f"Processing {result['urn']}")
#    logger.warning(json.dumps(result))
    if "subscriptionInfo" not in result:
        logger.warning(f"Skipping: {result['urn']} because no subscription information was found")
        continue
    
    subscriptionInfo: SubscriptionInfoClass = SubscriptionInfoClass.from_obj(result["subscriptionInfo"]["value"])
    subscriptionNotificationConfig: SubscriptionNotificationConfigClass = subscriptionInfo.notificationConfig
    logger.debug(f"subscriptionNotificationConfig: {json.dumps(subscriptionNotificationConfig.to_obj())}")
    if subscriptionNotificationConfig.notificationSettings.slackSettings is not None:
        slackSettings: SlackNotificationSettingsClass = subscriptionNotificationConfig.notificationSettings.slackSettings
        if slackSettings.channels:
            unique_channels.update(slackSettings.channels)
        
print(json.dumps(list(unique_channels)))