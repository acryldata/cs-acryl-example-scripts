import os
import json
import logging
from typing import Any
from collections import deque
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph
)

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO,
    format='%(message)s'
)

# Utility to get path of executed script regardless of where it is executed from
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def find_urn_li_strings(data):
    result = []
    
    def recursive_search(obj):
        if isinstance(obj, dict):
            # Search through dictionary values
            for value in obj.values():
                recursive_search(value)
        elif isinstance(obj, list):
            # Search through list items
            for item in obj:
                recursive_search(item)
        elif isinstance(obj, str) and obj.startswith("urn:li:"):
            # Found a matching string
            result.append(obj)
    
    recursive_search(data)
    return result


def download_urn(client, data, queue, root_urn):

    if root_urn in data:
        logger.warning(f"Skipping {root_urn}")
        return

    payload: dict = client.get_entity_raw(root_urn)

    urn = payload["urn"]
    aspects: dict = payload["aspects"]

    data[urn] = aspects
    #logger.info(f"Appending {urn} to our data object")

    for aspect in aspects:
        aspect_payload = aspects.get(aspect)
        urn_references = set(find_urn_li_strings(aspect_payload))
        for ref in urn_references:
            if ref not in data.keys():
                #logger.info(f"Added {ref} for processing")
                queue.append(ref)


## Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

# This is the full set of data to download.
# urn -> dict of aspects
data: dict[str, Any] = {}

# Used to process the list of refs to the asset to be downloaded
queue = deque()

# Urn to be downloaded + all it's dependencies (only 1 layer)
# TODO: This might fail on things like glossary terms having the glossary group references, same thing for domains.
root_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)"

download_urn(client, data, queue, root_urn)

# Process the queue
while queue:
    urn = queue.popleft()
    logger.info(f"Processing: {urn}")
    download_urn(client, data, deque(), urn)


with open('data.json', 'w') as file:
    json.dump(data, file, indent=4)