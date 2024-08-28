import json
import logging
from itertools import groupby
from typing import List, Dict
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.schema_classes import DatasetKeyClass
from datahub.utilities.urns.urn import guess_entity_type
from tqdm import tqdm
from rich.progress import Progress
from rich.logging import RichHandler


#logging.basicConfig(level=logging.INFO, handlers=[RichHandler()])
logging.basicConfig(
    level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger("rich")

def batch_iter(iterable, batch_size):
    """
    Utility to split an iterable into batches of a specified size. 

    Args:
        iterable: The iterable to be batched.
        batch_size: The size of each batch.

    Yields:
        Batches of the iterable.
    """
    current_batch = []
    for item in iterable:
        current_batch.append(item)
        if len(current_batch) == batch_size:
            yield current_batch
            current_batch = []
    if current_batch:
        yield current_batch
        

def group_by(objects, key_func) -> Dict:
    return {k: list(v) for k, v in groupby(sorted(objects, key=key_func), key=key_func)}


def get_execution_runs(client: DataHubGraph, ingestion_urn: str) -> List[str]:
    # Filter to pull subscriptions that have a running status
    filters = []
    filter_criteria: List[SearchFilterRule] = [
        {
            "field": "ingestionSource",
            "condition": "EQUAL",
            "values": [ingestion_urn],
        }
    ]

    filters.append({"and": filter_criteria})

    # Get execution requests, sorted by time.
    search_body = {
        "input": "*",
        "entity": "dataHubExecutionRequest",
        "start": 0, 
        "count": 1,
        "filter": {"or": filters},
        "sort": {"field": "requestTimeMs", "order": "DESCENDING"},
    }

    results: Dict = client._post_generic(endpoint, search_body)
    num_entities = results.get("value", {}).get("numEntities", 0)
    if num_entities == 0:
        logger.warning(
            f"No executions found for {ingestion_urn}"
        )
        exit(0)
        
    search_body["count"] = num_entities
    results: Dict = client._post_generic(endpoint, search_body)

    return[entry["entity"] for entry in results["value"]["entities"]]

# Connect to the DataHub instance configured in your ~/.datahubenv file.
datahub: DataHubGraph = get_default_graph()

# TODO fill this
ingestion="urn:li:dataHubIngestionSource:snowflake_queries"

dry_run = True

# Get all executions of this ingestion source
# OpenAPI approach
endpoint = f"{datahub.config.server}/entities?action=search"

urns_deleted: int = 0
with Progress() as progress:
    ingestion_run_urns = [ingestion_run_urn.split(':')[-1] for ingestion_run_urn in get_execution_runs(datahub, ingestion)]
    logger.info(f"Ingestion runs: {json.dumps(ingestion_run_urns)}")
    task = progress.add_task("Processing execution requests...", total=len(ingestion_run_urns))
    for ingestion_run_urn in ingestion_run_urns:
        progress.update(task, advance=1, description=str(f"Processing: urn:li:dataHubExecutionRequest:{ingestion_run_urn}"))

        # GraphQL approach to pull all entities that were updated by the ingestion run, not necessarily the key aspect
        urn_batches = batch_iter(datahub.get_urns_by_filter(
            query=f"/q runId:{ingestion_run_urn}",
        ), 1000)

        for batch in urn_batches:
            urns_by_type = group_by(batch, guess_entity_type)
            for entity_type in urns_by_type.keys():
                if entity_type == "dataset":
                    response = datahub.get_entities_v2(entity_name=entity_type, urns=urns_by_type[entity_type], aspects=["datasetKey"], with_system_metadata=True)
                    for key in response:
                        if "systemMetadata" not in response[key]['datasetKey']:
                            continue
                        if response[key]['datasetKey']['systemMetadata']['lastRunId'] in ingestion_run_urns:
                            if dry_run:
                                logger.info(f"Would delete: {key} and {datahub.delete_references_to_urn(key, True)[0]} references")
                                urns_deleted = urns_deleted +1
                            else:
                                datahub.delete_entity(key)
                                refs = datahub.delete_references_to_urn(key)
                                logger.info(f"Deleted: {key} and {refs.count} references")
                                urns_deleted = urns_deleted +1
                        else:
                            logger.debug(f"Skipping {key} created by {response[key]['datasetKey']['systemMetadata']['lastRunId']}")
                else:
                    logger.warning(f"Found unprocessed type: {entity_type}")
                    
print(f"Script deleted {urns_deleted} assets")