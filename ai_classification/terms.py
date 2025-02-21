#!/usr/bin/env -S uv --quiet run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "acryl-datahub",
#     "acryl-datahub-cloud",
#     "loguru",
# ]
# ///

import acryl_datahub_cloud.metadata.schema_classes as models
import click
from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.urns import DatasetUrn
from loguru import logger

gql = """\
query listTermProposals($endTimestampMillis: Long, $count: Int) {
  listActionRequests(input: {
    type: TERM_ASSOCIATION
    status: PENDING
    count: $count
    endTimestampMillis: $endTimestampMillis
  }) {
    start
    count
    total
    actionRequests {
      urn
      type
      status
      entity {
        urn
      }
      subResourceType
      subResource
      params {
        glossaryTermProposal {
          glossaryTerm {
            urn
          }
        }
      }
      origin
      created {
        time
      }
    }
  }
}

mutation rejectProposal($urn: String!) {
  rejectProposal(urn: $urn)
}
"""


@click.group()
def main():
    pass


def _clear_classified_flag_mcp(urn: str) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=models.EntityInferenceMetadataClass(
            # TODO: Replace this with a patch.
            glossaryTermsInference=models.InferenceGroupMetadataClass(
                lastInferredAt=get_sys_time(),
                version=0,
            ),
        ),
    )


@main.command()
@click.option("--dry-run", is_flag=True, default=False)
def clear_all_proposals(dry_run: bool):
    graph = get_default_graph()
    logger.info(f"Using graph {graph}")

    proposals_rejected = 0
    end_timestamp = None
    while True:
        # Results are ordered by time, newest first.
        res = graph.execute_graphql(
            gql,
            operation_name="listTermProposals",
            variables={"endTimestampMillis": end_timestamp, "count": 100},
        )["listActionRequests"]
        if res["total"] == 0 or not (proposals := res["actionRequests"]):
            break

        for proposal in proposals:
            if proposal["origin"] != "INFERRED":
                continue

            action_request_urn = proposal["urn"]
            entity_urn = proposal["entity"]["urn"]
            logger.info(
                ("DRY RUN: " if dry_run else "")
                + f"Rejecting proposal {action_request_urn} on "
                f"{entity_urn} {proposal['subResourceType']} {proposal['subResource']} "
                f"to add {proposal['params']['glossaryTermProposal']['glossaryTerm']['urn']}"
            )
            proposals_rejected += 1

            if not dry_run:
                graph.execute_graphql(
                    gql,
                    operation_name="rejectProposal",
                    variables={"urn": action_request_urn},
                )

        end_timestamp = proposals[-1]["created"]["time"]

    logger.info(
        ("DRY RUN: " if dry_run else "") + f"Rejected {proposals_rejected} proposals"
    )


@main.command()
@click.option("--dry-run", is_flag=True, default=False)
def clear_all_flags(dry_run: bool):
    graph = get_default_graph()
    logger.info(f"Using graph {graph}")

    cleared = 0
    with graph.make_rest_sink() as sink:
        for urn in graph.get_urns_by_filter(
            entity_types=[DatasetUrn.ENTITY_TYPE],
            batch_size=100,
            extraFilters=[
                {
                    "field": "glossaryTermsVersion",
                    "values": ["0"],
                    "condition": "GREATER_THAN",
                }
            ],
        ):
            logger.info(f"Clearing flag for {urn}")
            if not dry_run:
                sink.emit_async(_clear_classified_flag_mcp(urn))
            cleared += 1

    logger.info(
        ("DRY RUN: " if dry_run else "") + f"Cleared flags on {cleared} entities"
    )


@main.command()
@click.option("--urn", required=True)
def clear_single_flag(urn: str):
    assert DatasetUrn.from_string(urn)

    graph = get_default_graph()
    logger.info(f"Using graph {graph}")

    graph.emit_mcp(_clear_classified_flag_mcp(urn))


if __name__ == "__main__":
    main()
