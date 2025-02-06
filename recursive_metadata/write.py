import os
import sys
import logging
import json
from dataclasses import dataclass
from avro.schema import RecordSchema
from importlib.metadata import version
from datahub.metadata.schema_classes import (
    KEY_ASPECTS,
    ASPECT_NAME_MAP,
)
from datahub.utilities.urns.urn import guess_entity_type
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph


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


logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format="%(message)s")

# Utility to get path of executed script regardless of where it is executed from
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


def putEntity(client: DataHubGraph, entity: str, payload: dict, urn: str):
    endpoint = f"{client.config.server}/openapi/v3/entity/{entity}?async=false&systemMetadata=false"

    # Add urn to the payload as it is something specific to the openAPI endpoints
    payload["urn"] = urn
    client._post_generic(endpoint, [payload])

    # logger.warning(json.dumps(response))


def sortByKeyAspectCreatedDate(
    urn: str, data: dict[str, dict[str, object]], index: DataHubIndex
) -> int:
    entity_type = guess_entity_type(urn)

    registry_entry: DataHubRegistryEntityEntry = index.registry.get(entity_type)

    if not registry_entry:
        logger.error(f"No registry entry for {urn} found, can't compute keyAspect")
        exit(0)
        return sys.maxsize

    key_aspect_name = registry_entry.key
    key_aspect: dict = data.get(urn).get(key_aspect_name)

    if not key_aspect:
        logger.error(f"Key aspect {key_aspect_name} for {urn} not found")
        exit(0)
        return sys.maxsize

    if "created" not in key_aspect.keys():
        logger.error(f"Key aspect for {urn} does not have created metadata")
        exit(0)
        return sys.maxsize

    created_metadata: dict = key_aspect["created"]

    if "time" not in created_metadata.keys():
        logger.error(
            f"Key aspect for {urn} does not have time defined in the created metadata"
        )
        exit(0)
        return sys.maxsize

    # logger.info(f"{urn} was created at {created_metadata['time']}")
    # exit(0)
    return int(created_metadata["time"])


file_name = "data.json"

if __name__ == "__main__":
    with open(file_name) as f:
        # This should be a dictionary of <urn: list<aspects>>
        data: dict = json.loads(f.read())

        ## Load DataHub's Entity Registry
        index: DataHubIndex = load_entity_registry()

        # Connect to the DataHub instance configured in your ~/.datahubenv file.
        client: DataHubGraph = get_default_graph()

        # Sort entities to write to datahub by the key aspect's created at timestamp.
        # This ensures that any dependency between entities is resolved by creation time.
        sorted_urns = sorted(
            data.keys(), key=lambda k: sortByKeyAspectCreatedDate(k, data, index)
        )

        # Pull structured properties related props and process them first to ensure dependencies on these properties work
        structured_props_instances = [key for key in data.keys() if "structured" in key]

        for key in structured_props_instances:
            entity_type = guess_entity_type(key)
            putEntity(client, entity_type, data.get(key), key)

        for key in sorted_urns:
            entity_type = guess_entity_type(key)
            putEntity(client, entity_type, data.get(key), key)
