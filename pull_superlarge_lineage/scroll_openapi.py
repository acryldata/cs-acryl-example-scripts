#!/usr/bin/env python3
import datetime
from collections import deque
from enum import Enum
from typing import Optional, Iterable, Set
from urllib.parse import quote

from datahub.ingestion.graph.client import DataHubGraph, get_default_graph


class RelationshipDirection(Enum):
    INCOMING = "INCOMING"
    OUTGOING = "OUTGOING"

# set below for upstream lineage
# RELATIONSHIP_TYPES = {
#     RelationshipDirection.INCOMING: [],
#     RelationshipDirection.OUTGOING: ["DownstreamOf", "Consumes"]
# }


# below will retrieve downstream lineage
RELATIONSHIP_TYPES = {
    RelationshipDirection.INCOMING: ["DownstreamOf", "Consumes"],
    RelationshipDirection.OUTGOING: []
}

RELATIONSHIP_OTHER_EDGE_REF = {
    RelationshipDirection.INCOMING: "source",
    RelationshipDirection.OUTGOING: "destination"
}


class LineageOpenAPIRetriever:
    def __init__(self):
        self.client: DataHubGraph = get_default_graph()
        self.endpoint: str = f"{self.client.config.server}/openapi/v3/relationship"
        self.urns_with_exceptions = []

    def get_lineage(self, target_urn: str, filename: str, max_depth: int = 3):
        visited_urns: Set[(int, str)] = set()
        urns_to_visit = deque([(1, target_urn)])
        current_depth = 0
        with open(filename, 'w') as f:
            while urns_to_visit:
                depth, current_urn = urns_to_visit.popleft()
                if depth > current_depth:
                    f.write(f"# Level of lineage: {depth}\n")
                    current_depth = depth
                visited_urns.add(current_urn)

                for direction in [RelationshipDirection.INCOMING, RelationshipDirection.OUTGOING]:
                    urns = self.scroll_entity(current_urn, direction)
                    f.write(f"# {datetime.datetime.now()} Direction of lineage: {direction}\n")
                    f.write(f"# Relations: {RELATIONSHIP_TYPES[direction]}\n")
                    for urn in urns:
                        f.write(f"{urn}\n")
                        if depth < max_depth and urn not in visited_urns:
                            urns_to_visit.append((depth + 1, urn))
                            visited_urns.add(urn)
                    f.write(f"# Urns to visit count: {len(urns_to_visit)}\n")
        if self.urns_with_exceptions:
            print(f"= Encountered {len(self.urns_with_exceptions)} exceptions when retrieving the lineage, for below urns:")
            for urn in self.urns_with_exceptions:
                print(f"! {urn}")

    def scroll_entity(self, urn: str, relationship_direction: RelationshipDirection) -> Iterable:
        if not RELATIONSHIP_TYPES[relationship_direction]:
            return
        address = f"{self.endpoint}/dataset/{quote(urn, safe='')}"
        print(f"=== Scrolling through entity: {urn} with direction: {relationship_direction}")

        scroll_id: Optional[str] = None
        variables = {
            "relationshipType[]": RELATIONSHIP_TYPES[relationship_direction],
            "direction": relationship_direction.value,
            "count": 3000
        }
        iter_count = 0
        while True:
            iter_count += 1
            variables["scrollId"] = scroll_id

            try:
                response: dict = self.client._get_generic(address, variables)
            except Exception as e:
                print(f"=== Got exception {e} when trying to retrieve relationships for {urn}, for {iter_count} iteration, stopping iteration for this entity")
                self.urns_with_exceptions.append(urn)
                break

            scroll_id = response.get("scrollId", None)
            results = response.get("results", [])
            if not results:
                print(f"===== Got empty results, finishing the scroll")
                return

            for result in results:
                node = result[RELATIONSHIP_OTHER_EDGE_REF[relationship_direction]]
                if node['entityType'] in ["dataset", "dataJob", "dataFlow", "dashboard"]:
                    yield node['urn']
                else:
                    print(f"======= Ignored entity type: {node['entityType']} | urn: {node.get('urn')}")

            print(f"===== Iteration no {iter_count}, retrieved results = {len(results)}")

            if not scroll_id:
                print("===== Next scrollId is None, finishing scroll")
                break


retriever = LineageOpenAPIRetriever()
retriever.get_lineage("urn:li:dataset:(urn:li:dataPlatform:s3,raw-data-bucket/orders/2024/customer_orders.csv,PROD)", "test.log", max_depth=5)