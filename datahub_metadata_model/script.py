import os
import logging
from dataclasses import dataclass
from avro.schema import RecordSchema
from importlib.metadata import version
from datahub.metadata.schema_classes import (
    KEY_ASPECTS,
    ASPECT_NAME_MAP,
)

logger = logging.getLogger(__name__)

# Utility to get path of executed script regardless of where it is executed from
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


@dataclass
class DataHubRegistryEntityEntry:
    # Category of the DataHub Entity
    category: str
    # Name of the key aspect for the DataHub Entity
    key: str
    # List of aspects that make up the DataHub Entity
    aspects: list[str]


@dataclass
class RelationshipInfo:
    # Name of the relationship (e.g., "Has", "OwnedBy")
    name: str
    # Entity types this relationship can reference
    entity_types: list[str]


@dataclass
class DataHubIndex:
    # Index of entities that exist with their aspect names
    registry: dict[str, DataHubRegistryEntityEntry]
    # Schemas of aspects
    schemas: dict[str, RecordSchema]
    # Map of aspect names to their relationships
    relationships: dict[str, list[RelationshipInfo]]


def load_entity_registry() -> DataHubIndex:
    registry: dict[str, DataHubRegistryEntityEntry] = {}

    schemas: dict[str, RecordSchema] = {}

    relationships: dict[str, list[str]] = {}

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

    # Extract relationships from schemas
    for aspect_name, schema in schemas.items():
        relationship_infos = _extract_relationships(schema)
        if relationship_infos:
            relationships[aspect_name] = relationship_infos

    logger.info("Finished loading DataHub's Entity Registry")

    return DataHubIndex(registry=registry, schemas=schemas, relationships=relationships)


def _extract_relationships(schema: RecordSchema) -> list[RelationshipInfo]:
    """Extract all Relationships from a schema"""
    relationships = []

    def process_field(field):
        # Get the field as a JSON dict to examine its properties
        field_dict = field.to_json()

        # Check if this field has a Relationship property
        if isinstance(field_dict, dict) and 'Relationship' in field_dict:
            relationship = field_dict['Relationship']
            if isinstance(relationship, dict):
                rel_name = relationship.get('name', '')
                entity_types = relationship.get('entityTypes', [])
                if rel_name and entity_types:
                    relationships.append(RelationshipInfo(
                        name=rel_name,
                        entity_types=entity_types
                    ))

        # Recursively process nested record types
        field_type = field.type
        if hasattr(field_type, 'fields'):
            # It's a record type
            for nested_field in field_type.fields:
                process_field(nested_field)
        elif hasattr(field_type, 'schemas'):
            # It's a union type
            for schema_option in field_type.schemas:
                if hasattr(schema_option, 'fields'):
                    for nested_field in schema_option.fields:
                        process_field(nested_field)
                elif hasattr(schema_option, 'items') and hasattr(schema_option.items, 'fields'):
                    # It's an array of records
                    for nested_field in schema_option.items.fields:
                        process_field(nested_field)
        elif hasattr(field_type, 'items') and hasattr(field_type.items, 'fields'):
            # It's an array type with record items
            for nested_field in field_type.items.fields:
                process_field(nested_field)

    for field in schema.fields:
        process_field(field)

    return relationships


## Load DataHub's Entity Registry
index: DataHubIndex = load_entity_registry()

print("=== Actors Schema ===")
print(index.schemas.get("actors"))
print("\n=== Actors Relationships ===")
actors_rels = index.relationships.get("actors", [])
for rel in actors_rels:
    print(f"  {rel.name}: {rel.entity_types}")

print("\n=== All Relationships ===")
for aspect_name, relationships in sorted(index.relationships.items()):
    print(f"\n{aspect_name}:")
    for rel in relationships:
        print(f"  {rel.name}: {rel.entity_types}")
