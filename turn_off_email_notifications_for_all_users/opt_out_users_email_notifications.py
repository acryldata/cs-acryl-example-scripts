#!/usr/bin/env python3
import datetime
from typing import Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import CorpUserSettingsClass

graph = get_default_graph()

users = [*graph.get_urns_by_filter(entity_types=["CORP_USER"], batch_size=1000)]
print(f"Retrieved {len(users)} users")

no_aspect_count = 0
no_change_count = 0
change_count = 0

for i, user in enumerate(users):
    if i % 50 == 0:
        print(f"{datetime.datetime.now()} Processed {i} users")
    aspect: Optional[CorpUserSettingsClass] = graph.get_aspect(user, CorpUserSettingsClass)
    if not aspect:
        print(f"Couldn't retrieve CorpUserSettings aspect for user: {user}")
        no_aspect_count += 1
        continue
    assert aspect
    changed = False
    settings = aspect.notificationSettings

    if 'EMAIL' in settings.sinkTypes:
        settings.sinkTypes.remove('EMAIL')
        changed = True

    if changed:
        graph.emit_mcp(MetadataChangeProposalWrapper(entityUrn=user, aspect=aspect))
        change_count += 1
    else:
        no_change_count += 1

print(f"===========\nRun statistics\n===========")
print(f"Users for which we didn't find CorpUserSettings aspect: {no_aspect_count}")
print(f"Users for which we didn't change CorpUserSettings aspect: {no_change_count}")
print(f"Users for which changed CorpUserSettings aspect: {change_count}")
