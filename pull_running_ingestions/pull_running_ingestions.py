from typing import List, Optional
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.metadata.schema_classes import ExecutionRequestResultClass, ExecutionRequestInputClass

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

# Filter to pull dataHubExecutionRequest that have a running status
filters: List[SearchFilterRule] = [
    {
        "field": "executionResultStatus",
        "condition": "EQUAL",
        "values": ["RUNNING"],
        "negated": False,
    }
]

urns = client.get_urns_by_filter(
    query="*",
    entity_types=["dataHubExecutionRequest"],
    extraFilters=filters)

# Pull down the execution result aspect for each execution request that is currently running.
for urn in urns:
    execution_input: Optional[ExecutionRequestInputClass] = client.get_aspect(urn=urn, aspect_type=ExecutionRequestInputClass)
    type
    if not execution_input:
        print(f"No execution request input found for {urn}, skipping")
    
    if (execution_input.task != "RUN_INGEST"):
        print(f"Skipping task unrelated to ingestion {urn}")
        continue
    
    execution_request: Optional[ExecutionRequestResultClass] = client.get_aspect(urn=urn, aspect_type=ExecutionRequestResultClass)
    if not execution_request:
        print(f"No execution request result found for {urn}")
    
    print(execution_request)