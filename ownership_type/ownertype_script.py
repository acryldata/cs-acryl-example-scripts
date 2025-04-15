#!/usr/bin/env python

import progressbar
from typing import Iterable
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata._schema_classes import (
    OwnershipClass,
    OwnerClass,
    OwnershipSourceClass,
    OwnershipTypeClass,
)

# setup below
TOKEN = "xxx"
HOSTNAME = "https://xxx.acryl.io/gms"

graph = DataHubGraph(
    config=DatahubClientConfig(server=HOSTNAME, token=TOKEN if TOKEN else None)
)
emitter = DataHubRestEmitter(gms_server=f"{HOSTNAME}", token=TOKEN if TOKEN else None)

scroll_container_query = """
query scrollAcrossEntities($input: ScrollAcrossEntitiesInput!) {
  scrollAcrossEntities(input: $input) {
    nextScrollId
    count
    total
    searchResults {
      entity {
        urn
      }
    }
  }
}
"""


search_query = """
query getContainer($urn: String!) {
  container(urn: $urn) {
    entities(input: {start: 0, count: 10000}) {
      total
      searchResults {
        entity {
          urn
          ... on Dataset {
            ownership {
              owners {
                owner {
                  __typename
                  ... on CorpUser {
                    urn
                  }
                  ... on CorpGroup {
                    urn
                  }
                }
                ownershipType {
                  urn
                  type
                  info {
                    name
                  }
                }
                source {
                  type
                  url
                }
              }
              lastModified {
                time
                actor
              }
            }
          }
        }
      }
      __typename
    }
  }
}
"""


dataset_done = set()


def get_container_urns() -> Iterable[str]:
    results = graph.execute_graphql(
        query=scroll_container_query,
        variables={"input": {"query": "*", "types": ["CONTAINER"], "count": 1000}},
    )
    scroll_across_entities = results["scrollAcrossEntities"]
    scroll_id = scroll_across_entities["nextScrollId"]
    while scroll_id:
        for search_result in scroll_across_entities["searchResults"]:
            yield search_result["entity"]["urn"]
        results = graph.execute_graphql(
            query=scroll_container_query,
            variables={
                "input": {
                    "query": "*",
                    "types": ["CONTAINER"],
                    "scrollId": scroll_id,
                    "count": 1000,
                },
            },
        )
        scroll_across_entities = results["scrollAcrossEntities"]
        scroll_id = scroll_across_entities["nextScrollId"]


def main():
    for container in get_container_urns():
        print(f"\n{'=' * 20}\nProcessing container with urn: {container}")
        results = graph.execute_graphql(
            query=search_query, variables={"urn": container}
        )
        datasets = results["container"]["entities"]["searchResults"]
        print(f"Amount of retrieved datasets for the container: {len(datasets)}")
        for dataset in progressbar.progressbar(datasets):
            if "ownership" not in dataset:
                continue
            dataset = dataset["entity"]
            urn = dataset["urn"]
            if urn in dataset_done:
                continue
            dataset_done.add(urn)
            print(f"{'-' * 20}\nProcessing dataset with urn: {urn}")
            owner_aspects = []
            owners = dataset["ownership"]["owners"]
            for owner in owners:
                owner_urn = owner["owner"]["urn"]
                ownership_type_urn = owner["ownershipType"]["urn"]
                if owner["ownershipType"]["type"] == "CUSTOM_OWNERSHIP_TYPE":
                    ownership_type_type = OwnershipTypeClass.CUSTOM
                else:
                    print(
                        f"\n\nWARNING\nDidn't recognize ownership type for user owner_urn and ownership type: {owner['ownershipType']['type']}\n\n"
                    )

                ownership_type_name = owner["ownershipType"]["info"]["name"]
                ownership_source = owner["source"]

                print(
                    f"Owner urn: {owner_urn}, type urn: {ownership_type_urn} | type type: {ownership_type_type} | type name: {ownership_type_name}, source: {ownership_source}"
                )
                owner_aspect = OwnerClass(
                    owner=owner_urn,
                    type=ownership_type_type,
                    typeUrn=ownership_type_urn,
                    source=OwnershipSourceClass(**ownership_source)
                    if ownership_source
                    else None,
                )
                print(f"Result class: {owner_aspect}")
                owner_aspects.append(owner_aspect)

            ownership_aspect = OwnershipClass(owners=owner_aspects)
            mcpw = MetadataChangeProposalWrapper(entityUrn=urn, aspect=ownership_aspect)
            print(f"Result ownership mcp to emit: {mcpw}")
            emitter.emit(mcpw)


if __name__ == "__main__":
    main()
