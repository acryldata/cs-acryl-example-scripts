import json
import logging
from typing import (
    Iterable, 
    Optional
)
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper

from datahub.metadata.schema_classes import GlobalTagsClass, CorpGroupInfoClass

logger = logging.getLogger(__name__)

dry_run: bool = True

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

tag_prefix = "data__producer__owner__"

# Get all tags that have:
# the tag prefix "data__producer__owner__"
# AND
# "email"
# as part of the urn
tag_urns: Iterable[str] = client.get_urns_by_filter(
    query=f'/q urn: *{tag_prefix}* AND urn: *email*', 
    entity_types=["tag"]
)

# For each relevant data producer owner's email tag:
# - Get 1 dataset that contains the tag to pull group information from it
# - Pull all tags for that dataset that have the tag prefix
# - Generate group information based on those tags
# - Push group information to DataHub
# 
# Assumption: Other group related information (team name & slack channel) exists in the same dataset as tags
for tag_urn in tag_urns:
    
    dataset_urns: Iterable[str] = client.get_urns_by_filter(
        entity_types=["dataset"], 
        extra_or_filters=[{
            "field": "tags",
            "condition": "IN",
            "values": [tag_urn],
            }],
        batch_size=1)
    
    if not dataset_urns:
        logger.info(f"No dataset found for tag: {tag_urn}")
        continue
    
    dataset_urn: str = next(iter(dataset_urns))
    
    # Get all tags from the dataset that has the email data producer tag
    tag_aspect: Optional[GlobalTagsClass] = client.get_tags(dataset_urn)
    
    if not tag_aspect:
        logger.warning(f"No tags found for dataset {dataset_urn}, this should not be happening as we are pulling datasets based on whether they have tags. Please reach out to Acryl to debug.")
        
    team_name: str = None
    team_slack: str = None
    
    # Extract team name & slack from the tags that exist in the dataset
    for tag_association in tag_aspect.tags:
        tag: str = tag_association.tag
        if "team__name" in tag:
            team_name = (
                tag.replace("urn:li:tag:", "")
                .replace(tag_prefix, "")
                .replace("team__name:", "")
                .replace("-_", " ")
                .title()
            )
        if "slack__channel" in tag:
            team_slack = (
                tag.replace("urn:li:tag:", "")
                .replace(tag_prefix, "")
                .replace("slack__channel:_-", "")
                .replace("--", "-")
            )
            team_slack = f"#{team_slack}"
    email: str = (
        tag_urn.replace("urn:li:tag:", "")
        .replace(tag_prefix + "email:", "")
        .replace("-nytimes_com", "@nytimes.com")
        .replace("--", "-")
    )
        
    group_urn: str = f"urn:li:corpGroup:{email}"
    mcp: MetadataChangeProposalWrapper =  MetadataChangeProposalWrapper(
        entityUrn=group_urn,
        aspect=CorpGroupInfoClass(
            displayName=team_name,
            slack=team_slack,
            email=email,
            admins=[],
            members=[],
            groups=[],
        ),
    )
    
    if dry_run:
        logger.warning(f"{json.dumps(mcp.aspect.to_obj())}")
    else:
        client.emit(mcp)
