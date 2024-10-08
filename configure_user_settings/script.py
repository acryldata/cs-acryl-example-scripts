import os
import json
import logging
from typing import Optional
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    CorpUserSettingsClass,
    CorpUserAppearanceSettingsClass
)
logger = logging.getLogger(__name__)

# Utility to get path of executed script regardless of where it is executed from
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

dry_run: bool = True

# TODO fill this out
user_urn = ""
    
if not client.exists(user_urn):
    logger.error(f"{user_urn} does not exist")

settings: Optional[CorpUserSettingsClass] = client.get_aspect(user_urn, aspect_type=CorpUserSettingsClass)

if not settings:
    settings = CorpUserSettingsClass(appearance=CorpUserAppearanceSettingsClass(showThemeV2=True))

if not settings.appearance: 
    settings.appearance = CorpUserAppearanceSettingsClass(showThemeV2=True)
else:
    settings.appearance.showThemeV2=True
        
mcp: MetadataChangeProposalWrapper =  MetadataChangeProposalWrapper(
        entityUrn=user_urn,
        aspect=settings
    )    

if dry_run:
    logger.warning(f"{json.dumps(mcp.to_obj())}")
else:
    client.emit(mcp, async_flag=False)
