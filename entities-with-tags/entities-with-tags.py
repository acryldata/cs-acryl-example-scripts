from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.emitter.mce_builder import make_data_platform_urn, make_tag_urn
from typing import List, Optional, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_search_query() -> str:
    """Build the GraphQL query for searching entities."""
    return """
    query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
        searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
                entity {
                    urn
                    type
                    ... on Dataset {
                        name
                        platform {
                            name
                        }
                        properties {
                            name
                            qualifiedName
                        }
                        globalTags {
                            tags {
                                tag {
                                    name
                                }
                            }
                        }
                        subTypes {
                            typeNames
                        }
                    }
                }
            }
        }
    }
    """


def build_search_input(
        tag_name: str,
        platform_code: str,
        subtype: Optional[str] = None,
        start: int = 0,
) -> Dict[str, Any]:
    """Build the input parameters for the search query."""
    and_filters = [
        {
            "field": "platform",
            "condition": "EQUAL",
            "values": [make_data_platform_urn(platform_code)],
            "negated": False
        },
        {
            "field": "tags",
            "condition": "EQUAL",
            "values": [make_tag_urn(tag_name)],
            "negated": False
        },
        {
            "field": "_entityType",
            "values": ["DATASET"]
        }
    ]

    if subtype:
        and_filters.append({
            "field": "typeNames",
            "values": [subtype],
            "condition": "EQUAL",
            "negated": False
        })

    return {
        "input": {
            "types": [],
            "query": "",
            "start": start,
            "count": 1000,
            "filters": [],
            "orFilters": [{"and": and_filters}]
        }
    }


def process_entity_info(entity: Dict[str, Any]) -> Dict[str, Any]:
    """Process entity information into a standardized format."""
    return {
        "urn": entity.get("urn"),
        "name": entity.get("name"),
        "platform": entity.get("platform", {}).get("name"),
        "qualified_name": entity.get("properties", {}).get("qualifiedName"),
        "tags": [
            tag["tag"]["name"]
            for tag in entity.get("globalTags", {}).get("tags", [])
            if tag.get("tag", {}).get("name")
        ],
        "subtypes": entity.get("subTypes", {}).get("typeNames", [])
    }


def get_entities_by_filters(
        tag_name: str,
        platform_code: str,
        graph: DataHubGraph,
        subtype: Optional[str] = None,
        output_to_std_out: bool = False,
) -> Optional[List[Dict[str, Any]]]:
    """Get entities using search query with filters."""
    try:

        query = build_search_query()
        entities_info = []
        start = 0

        while True:
            variables = build_search_input(tag_name, platform_code, subtype, start)
            result = graph.execute_graphql(query, variables=variables)

            if not result or "searchAcrossEntities" not in result:
                logger.error("Invalid response from GraphQL query")
                return None

            search_response = result["searchAcrossEntities"]
            search_results = search_response["searchResults"]
            total_results = search_response["total"]

            for result in search_results:
                entity = result.get("entity")
                if entity:
                    entity_info = process_entity_info(entity)
                    entities_info.append(entity_info)

                    if output_to_std_out:
                        logger.info(f"Dataset: {entity_info['name']}")
                        logger.info(f"URN: {entity_info['urn']}")
                        logger.info(f"Platform: {entity_info['platform']}")
                        logger.info(f"Subtypes: {', '.join(entity_info['subtypes'])}")
                        logger.info(f"Tags: {', '.join(entity_info['tags'])}")
                        logger.info("---")

            # Check if we've received all results
            if start + len(search_results) >= total_results:
                break

            # Increment start for next batch
            start += len(search_results)

        logger.info(f"Total results found: {len(entities_info)}")
        return entities_info

    except Exception as e:
        logger.error(f"Error getting entities: {str(e)}")
        return None


def main():
    """Main execution function."""
    
    graph = get_default_graph()
    
    # Example using Snowflake platform
    dremio_results = get_entities_by_filters(
        tag_name="Usage - B",
        platform_code="snowflake",
        graph=graph,
        subtype="View",
        output_to_std_out=True,
    )
    logger.info(f"Dremio results: {dremio_results}")

    # Example using BigQuery platform
    bigquery_results = get_entities_by_filters(
        tag_name="__default_high_queries",
        platform_code="bigquery",
        graph=graph,
        #subtype="View",
        #output_to_std_out=True,
    )
    logger.info(f"BigQuery results: {bigquery_results}")


if __name__ == "__main__":
    main()