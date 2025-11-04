# Deletes all smart assertions in an instance + associated monitors

import argparse
import logging
import os
from typing import Optional
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from rich.progress import Progress
from rich.logging import RichHandler

# Configure logging with RichHandler
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()]
)

logger = logging.getLogger("rich")


def parse_args():
    parser = argparse.ArgumentParser(
        description='Delete all smart assertions in a DataHub instance and their associated monitors'
    )
    parser.add_argument(
        '--gms-endpoint',
        type=str,
        default=os.getenv('DATAHUB_GMS_URL'),
        help='DataHub GMS endpoint URL (default: DATAHUB_GMS_URL env var)'
    )
    parser.add_argument(
        '--token',
        type=str,
        default=os.getenv('DATAHUB_TOKEN'),
        help='DataHub access token (default: DATAHUB_TOKEN env var)'
    )
    parser.add_argument(
        '--dry-run',
        default=False,
        action='store_true',
        help='Run in dry-run mode without actually deleting anything'
    )
    return parser.parse_args()


args = parse_args()

# Connect to the DataHub instance
if not args.gms_endpoint:
    raise ValueError("GMS endpoint must be provided via --gms-endpoint or DATAHUB_GMS_URL environment variable")
if not args.token:
    raise ValueError("Token must be provided via --token or DATAHUB_TOKEN environment variable")

client = DataHubGraph(DatahubClientConfig(server=args.gms_endpoint, token=args.token))
dry_run: bool = args.dry_run

logger.info(f"Processing: {args.gms_endpoint}")

scroll_gql = """
query scrollAcrossEntities($input:  ScrollAcrossEntitiesInput!) {
    scrollAcrossEntities(input: $input){
        nextScrollId,
        count,
        total,
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
        total = data.get("total", 0)
        for entry in data["searchResults"]:
            yield entry["entity"], total


variables = {
    "input": {
        "query": "/q sourceType: INFERRED",
        "types": ["ASSERTION"],
        "sortInput": {"sortCriteria": [{"field": "urn", "sortOrder": "ASCENDING"}]},
    }
}

deleted_assertions = 0
deleted_monitors = 0
skipped_assertions = 0
processed_count = 0
max_total_seen = 0

with Progress() as progress:
    task = progress.add_task("Processing assertions...", total=None)

    for result, total in scrollGraphQL(client, scroll_gql, variables):
        assertion_urn = result["urn"]
        processed_count += 1

        # Track the maximum total we've seen (since total decreases as we delete)
        if total > 0:
            max_total_seen = max(max_total_seen, total + processed_count - 1)
            progress.update(task, total=max_total_seen, completed=processed_count, description=f"Processing assertions ({processed_count}/{max_total_seen})")
        else:
            progress.update(task, description=f"Processing assertions ({processed_count})")

        if "monitor" not in result:
            logger.warning(f"{assertion_urn} has no monitor")
            skipped_assertions += 1
            continue

        monitor = result["monitor"]

        if "relationships" not in monitor:
            logger.warning(f"{assertion_urn} has no monitor relationship")
            skipped_assertions += 1
            continue

        relationships = monitor["relationships"]

        if len(relationships) < 1:
            logger.warning(f"{assertion_urn} has an empty relationship list to monitors")
            skipped_assertions += 1
            continue

        for relationship in relationships:
            if "entity" not in relationship:
                logger.warning(
                    f"{assertion_urn} has an empty relationship entry: {relationship}"
                )
                continue

            monitor_obj = relationship["entity"]

            if "urn" not in monitor_obj:
                logger.warning(
                    f"{assertion_urn} has a monitor relationship with no urn: {monitor_obj}"
                )
                continue

            monitor_urn = monitor_obj["urn"]

            if dry_run:
                print(f"[DRY RUN] Would delete assertion {assertion_urn} & monitor: {monitor_urn}")
                deleted_monitors += 1
                continue

            if not dry_run:
                delete_monitor_variables = {"input": str(monitor_urn)}
                client.execute_graphql(delete_monitor_gql, delete_monitor_variables)
                deleted_monitors += 1

        if not dry_run:
            delete_assertion_variables = {"input": str(assertion_urn)}
            client.execute_graphql(delete_assertion_gql, delete_assertion_variables)
            deleted_assertions += 1
        elif dry_run:
            deleted_assertions += 1

# Print summary
logger.info("="*60)
logger.info("Summary:")
logger.info(f"  Total assertions processed: {processed_count}")
logger.info(f"  Assertions deleted: {deleted_assertions}")
logger.info(f"  Monitors deleted: {deleted_monitors}")
logger.info(f"  Assertions skipped: {skipped_assertions}")
if dry_run:
    logger.info("\n  [DRY RUN MODE - No actual deletions performed]")
logger.info("="*60)
