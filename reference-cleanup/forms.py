# Script to find and clean up all dangling form references
#
# It works by
# Scrolling through all entities that can have forms (this is based on the entity-registry, looking for the forms aspect)
# For each entity with forms, get complete and incomplete form urn references, collect this into a unique set
# For each urn reference, check if it exists
# If the reference does not, then run a delete references on that form urn.
# Dry-run mode is support through the dry-run delete reference endpoint.

import logging
import json
from dataclasses import dataclass
from avro.schema import RecordSchema
from importlib.metadata import version
from datahub.metadata.schema_classes import (
    KEY_ASPECTS,
    ASPECT_NAME_MAP,
)
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from rich.progress import Progress
from rich.logging import RichHandler
from rich.console import Console

console = Console()

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)

dry_run: bool = True

@dataclass
class DataHubRegistryEntityEntry:
    # Category of the DataHub Entity
    category: str
    # Name of the key aspect for the DataHub Entity
    key: str
    # List of aspects that make up the DataHub Entity
    aspects: list[str]


@dataclass
class DataHubIndex:
    # Index of entities that exist with their aspect names
    registry: dict[str, DataHubRegistryEntityEntry]
    # Schemas of aspects
    schemas: dict[str, RecordSchema]


def load_entity_registry() -> DataHubIndex:
    registry: dict[str, DataHubRegistryEntityEntry] = {}

    schemas: dict[str, RecordSchema] = {}

    datahub_version = version("acryl-datahub")
    logger.debug(f"Processing version {datahub_version}")

    for key in KEY_ASPECTS:
        if (
            KEY_ASPECTS[key].ASPECT_INFO
            and "keyForEntity" in KEY_ASPECTS[key].ASPECT_INFO
        ):
            entity_name: str = KEY_ASPECTS[key].ASPECT_INFO["keyForEntity"]
            key_aspect: str = KEY_ASPECTS[key].ASPECT_NAME
            category: str = KEY_ASPECTS[key].ASPECT_INFO["entityCategory"]

            aspects: list[str] = [key_aspect]
            aspects.extend(KEY_ASPECTS[key].ASPECT_INFO["entityAspects"])

            registry[entity_name] = DataHubRegistryEntityEntry(
                category=category, key=key_aspect, aspects=aspects
            )

            # Load aspect schemas
            for aspect_name in aspects:
                if aspect_name not in schemas:
                    aspect = ASPECT_NAME_MAP.get(aspect_name)
                    if aspect:
                        schemas[aspect_name] = aspect.RECORD_SCHEMA
                    else:
                        logger.warning(
                            f"Aspect: {aspect_name} not found in ASPECT_NAME_MAP"
                        )

    logger.info("Finished loading DataHub's Entity Registry")

    return DataHubIndex(registry=registry, schemas=schemas)

def get_entities_with_aspect(index: DataHubIndex, aspect_name: str) -> list[str]:
    return [key.lower() for key in index.registry.keys() if aspect_name in index.registry.get(key).aspects]

def get_graphql_type_implementation_values(client: DataHubGraph, type: str) -> list[str]:
        """Get all types that implement the Entity interface."""
        query = f"""
        {{
            __type(name: "{type}") {{
                name
                kind
                possibleTypes {{name}}
            }}
        }}
        """

        data = client.execute_graphql(query)
        possible_values = []
        
        for type_info in data['__type']['possibleTypes']:
            possible_values.append(type_info['name'])
        
        return sorted(possible_values)

def get_graphql_type_enum_values(client: DataHubGraph, type: str) -> list[str]:
        """Get all types that implement the Entity interface."""
        query = f"""
        {{
            __type(name: "{type}") {{
                name
                kind
                enumValues {{name}}
            }}
        }}
        """

        data = client.execute_graphql(query)
        possible_values = []
        
        for type_info in data['__type']['enumValues']:
            possible_values.append(type_info['name'])
        
        return sorted(possible_values)

def generate_scroll_across_entities_query(entity_types: list[str]) -> str:
    """
    Generate a GraphQL query for search results with entity-specific fragments.

    Args:
        entity_types: A list of entity type names to include in the query

    Returns:
        A string containing the complete GraphQL query with fragments for each entity type
    """
    base_query = """
    query scrollAcrossEntities($input:  ScrollAcrossEntitiesInput!) {
        scrollAcrossEntities(input: $input){
            nextScrollId,
            searchResults {
            entity {
                type
                %s
            }
        }
    }
}
    """

    fragments = []
    for entity_type in entity_types:
        fragment = f"""... on {entity_type} {{
          urn
          forms {{
            incompleteForms {{form {{urn}}}}
            completedForms {{form {{urn}}}}
          }}
        }}"""
        fragments.append(fragment)

    # Join all fragments with newlines and proper indentation
    joined_fragments = "\n        ".join(fragments)

    # Format the final query
    return base_query % joined_fragments

def scrollGraphQL(client: DataHubGraph, query: str, variables: dict):
    first_iter = True
    scroll_id: Optional[str] = None
    while first_iter or scroll_id:
        first_iter = False
        variables["input"]["scrollId"] = scroll_id
        response = client.execute_graphql(
            query,
            variables=variables,
        )
        data = response["scrollAcrossEntities"]
        scroll_id = data["nextScrollId"]
        for entry in data["searchResults"]:
            yield entry["entity"]

## Load DataHub's Entity Registry
index: DataHubIndex = load_entity_registry()

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

entities = get_entities_with_aspect(index, "forms")
logger.debug(f"Registry Entities: {entities}")

graphql_entity_names = get_graphql_type_implementation_values(client, "Entity")
logger.debug(f"GraphQL Entities: {graphql_entity_names}")

processed_entities = [name for name in graphql_entity_names if name.lower() in entities]

logger.debug(f"Computed Entities: {processed_entities}")

scroll_gql = generate_scroll_across_entities_query(processed_entities)
logger.debug(f"Query: {scroll_gql}")

graphql_enum_types = get_graphql_type_enum_values(client, "EntityType")

processed_types = [name for name in graphql_enum_types if name.lower() in entities]

logger.debug(f"Computed Entity Types: {processed_entities}")

variables = {
    "input": {
        "query":"*", 
        "types": processed_types,
        "sortInput":{ "sortCriteria": [
            {"field": "urn", "sortOrder": "ASCENDING"}
        ]},
        "orFilters": [{
            "and": [{
                "field": "completedForms",
                "condition": "EXISTS"
                }
            ]
        },{
            "and": [{
                "field": "incompleteForms",
                "condition": "EXISTS"
            }]
        }]
    }
}

logger.debug(f"Variables: {json.dumps(variables)}")

# Store results in a list to eagerly pull down all results, otherwise this script is a reading and 
# pushing deletes at the same time causing the elasticsearch scroll to be inconsistent
# 
scroll_results = list(scrollGraphQL(client, scroll_gql, variables))
logger.info(f"[{client.config.server}] Found {len(scroll_results)} entries with form references")
urn_refs: set = set()

with Progress(console=console) as progress:
    task1 = progress.add_task("Processing assets with form urn references.", total=len(scroll_results))
    for result in scroll_results:
        progress.update(
                task1,
                advance=1,
                description=str(
                    f"Processing asset: {result['urn']}"
                ),
            )
        if "forms" not in result:
            continue
        if result["forms"] is None:
            continue
        completedForms: list = result['forms']['completedForms']
        incompleteForms: list = result['forms']['incompleteForms']
        if completedForms is not None:
            urn_refs.update([obj["form"]["urn"] for obj in completedForms])
        if incompleteForms is not None:
            urn_refs.update([obj["form"]["urn"] for obj in incompleteForms])

    logger.info(f"[{client.config.server}] Found {len(urn_refs)} unique form references")

    task2 = progress.add_task("Checking if form urns exist.", total=len(urn_refs))
    for form_urn in urn_refs:
        progress.update(task2, advance=1, description=str(f"Checking form: {form_urn}"))
        if not client.exists(form_urn):
            logger.info(f"[{client.config.server}] Delete ghost urn reference: {form_urn}")
            if not dry_run:
                client.delete_references_to_urn(form_urn)
        