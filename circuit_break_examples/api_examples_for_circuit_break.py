import os
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from graphql.gql_variable_definitions import get_scroll_across_lineage_vars


def get_graph_query(file_path):
    with open(file_path, 'r') as f:
        query = f.read()
    return query


def get_upstreams(graph):

    sal_variables = get_scroll_across_lineage_vars("urn:li:dataset:(urn:li:dataPlatform:postgres,postgres.public.testtable,PROD)", "UPSTREAM", 100)
    gql_query = get_graph_query("/Users/ethancartwright/Desktop/customer_code_v2/cs-acryl-example-scripts/circuit_break_examples/graphql/scrollAcrossLineage.gql")
    upstreams = graph.execute_graphql(query=gql_query, variables=sal_variables)

    return upstreams


gms_endpoint = "https://dev01.acryl.io/gms"
token = os.getenv("DATAHUB_TOKEN")

# Initialize the graph
graph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token))

# Execute the query
upstreams = get_upstreams(graph)
print(upstreams)
