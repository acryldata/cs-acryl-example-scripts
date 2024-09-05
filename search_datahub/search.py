from typing import Optional, Dict, List, Iterable
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.ingestion.graph.client import (
    DataHubGraph,
    DatahubClientConfig
)
from datahub.metadata.schema_classes import (
    OwnershipClass, OwnerClass, DomainsClass
)

gms_endpoint: str = ""
token: str = ""
headers = Optional[Dict[str, str]] = None

graph: DataHubGraph = DataHubGraph(DatahubClientConfig(server=gms_endpoint, token=token, extra_headers=headers, disable_ssl_verification=False))


filters: List[SearchFilterRule] = [
    {
        "field": "domains",
        "condition": "EQUAL",
        "values": ["urn:li:domain:Ads"]
    },
    {
        "field": "owners",
        "condition": "EXISTS",
        "negated": False
    }
]

# get_urns_by_filter performs a scrollAcrossEntities that returns an iterable of urns, lazily loaded (this reduces load on DataHub)
# This access Elasticsearch
urns: Iterable[str] = graph.get_urns_by_filter(query="*", entity_types=["dataset"], batch_size=1000, filter=filters)

# For each returned result get the owner and domain aspects, by querying SQL
for urn in urns:
    ownership: Optional[OwnershipClass] = graph.get_aspect(urn, OwnershipClass)
    if not ownership:
        print(f"No ownership aspect found for {urn}")
    owners: List[OwnerClass] = ownership.owners()
    
    domains: Optional[DomainsClass] = graph.get_aspect(urn, DomainsClass)
    if not domains:
        print(f"No domain aspect found for {urn}")
    domains: List[str] = domains.domains()
    
    # Do whatever you want with the domains & owners of a dataset urn