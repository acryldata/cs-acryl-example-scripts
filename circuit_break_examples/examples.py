import os
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from graphql.gql_variable_definitions import (
    get_scroll_across_lineage_vars,
    get_dataset_assertions_vars,
    get_add_tag_vars,
    get_remove_tag_vars,
    get_assertion_details_vars,
    get_run_assertion_vars,
)
from typing import Dict, Any

GRAPHQL_QUERY_FOLDER_PATH = "/Users/ethancartwright/Desktop/customer_code_v2/cs-acryl-example-scripts/circuit_break_examples/graphql/"


def get_graph_query(file_path: str) -> str:
    with open(file_path, "r") as f:
        query = f.read()
    return query


def get_upstreams(graph: DataHubGraph) -> Dict[str, Any]:
    variables = get_scroll_across_lineage_vars(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,postgres.public.testtable,PROD)",
        "UPSTREAM",
        100,
    )
    gql_query = get_graph_query(GRAPHQL_QUERY_FOLDER_PATH + "scrollAcrossLineage.gql")
    response = graph.execute_graphql(query=gql_query, variables=variables)
    return response


def get_dataset_assertions(graph: DataHubGraph) -> Dict[str, Any]:
    variables = get_dataset_assertions_vars(
        "urn:li:dataset:(urn:li:dataPlatform:redshift,rs.dokken.prod.dokken_prod.wba_events.core_heartbeat,PROD)"
    )
    gql_query = get_graph_query(GRAPHQL_QUERY_FOLDER_PATH + "getDatasetAssertions.gql")
    response = graph.execute_graphql(query=gql_query, variables=variables)
    return response


def add_tag(graph: DataHubGraph, entity_urn: str, tag_urn: str) -> Dict[str, Any]:
    variables = get_add_tag_vars(entity_urn, tag_urn)
    gql_query = get_graph_query(GRAPHQL_QUERY_FOLDER_PATH + "addTag.gql")
    response = graph.execute_graphql(query=gql_query, variables=variables)
    return response


def remove_tag(graph: DataHubGraph, entity_urn: str, tag_urn: str) -> Dict[str, Any]:
    variables = get_remove_tag_vars(entity_urn, tag_urn)
    gql_query = get_graph_query(GRAPHQL_QUERY_FOLDER_PATH + "removeTag.gql")
    response = graph.execute_graphql(query=gql_query, variables=variables)
    return response


def get_assertion_details(graph: DataHubGraph, assertion_urn: str) -> Dict[str, Any]:
    variables = get_assertion_details_vars(assertion_urn)
    gql_query = get_graph_query(
        GRAPHQL_QUERY_FOLDER_PATH + "getAssertionWithMonitors.gql"
    )
    response = graph.execute_graphql(query=gql_query, variables=variables)
    return response


def run_assertion(graph: DataHubGraph, assertion_urn: str) -> Dict[str, Any]:
    variables = get_run_assertion_vars(assertion_urn)
    gql_query = get_graph_query(GRAPHQL_QUERY_FOLDER_PATH + "runAssertion.gql")
    response = graph.execute_graphql(query=gql_query, variables=variables)
    return response


gms_endpoint = "https://dev01.acryl.io/gms"
token = os.getenv("DATAHUB_TOKEN")

# Initialize the graph
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))

# Execute the query
upstreams_object = get_upstreams(graph)
assertion_object = get_dataset_assertions(graph)

assertions = assertion_object["dataset"]["assertions"]["assertions"]

# Filter assertions with active monitors
active_monitors = [
    assertion
    for assertion in assertions
    if assertion["monitor"]["info"]["status"]["mode"] == "ACTIVE"
]

tag_added = add_tag(
    graph,
    "urn:li:assertion:ID",
    "urn:li:tag:test",
)

tag_removed = remove_tag(
    graph,
    "urn:li:assertion:ID",
    "urn:li:tag:test",
)

assertion_details = get_assertion_details(graph, "urn:li:assertion:ID")

run_assertion_response = run_assertion(graph, "urn:li:assertion:ID")
