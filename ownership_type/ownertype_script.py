#!/usr/bin/env python

import progressbar
from typing import Iterable
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata._schema_classes import (
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
)

graph = get_default_graph()

dataset_done = set()


def get_datasets(platform: str) -> Iterable[str]:
    yield from graph.get_urns_by_filter(
        entity_types=["dataset"],
        platform=platform,
    )


def main():
    datasets = list(get_datasets(platform="snowflake"))
    for dataset_urn in progressbar.progressbar(datasets):
        entity = graph.get_entities(
            entity_name="dataset",
            urns=[dataset_urn],
        )
        for urn, vals in entity.items():
            if 'ownership' not in vals:
                break
            ownership = vals["ownership"]
            owners = ownership[0].owners
            break
        if urn in dataset_done:
            continue
        dataset_done.add(urn)
        
        owner_aspects = []
        for owner in owners:
            owner: OwnerClass
            owner_urn = owner.owner
            ownership_type_urn = owner.typeUrn
            if owner.type == "CUSTOM":
                ownership_type_type = OwnershipTypeClass.CUSTOM
            elif owner.type == "DATAOWNER":
                ownership_type_type = OwnershipTypeClass.DATAOWNER
            elif owner.type == "BUSINESS_OWNER":
                ownership_type_type = OwnershipTypeClass.BUSINESS_OWNER
            elif owner.type == "DATA_STEWARD":
                ownership_type_type = OwnershipTypeClass.DATA_STEWARD
            elif owner.type == "TECHNICAL_OWNER":
                ownership_type_type = OwnershipTypeClass.TECHNICAL_OWNER
            else:
                raise RuntimeError(f"Unknown ownership type: {owner.type} for {urn}")
            owner_aspect = OwnerClass(
                owner=owner_urn,
                type=ownership_type_type,
                typeUrn=ownership_type_urn,
                source=None,
            )
            owner_aspects.append(owner_aspect)

        ownership_aspect = OwnershipClass(owners=owner_aspects)
        mcpw = MetadataChangeProposalWrapper(entityUrn=urn, aspect=ownership_aspect)
        graph.emit(mcpw)


if __name__ == "__main__":
    main()
