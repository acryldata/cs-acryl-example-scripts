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


## Load DataHub's Entity Registry
index: DataHubIndex = load_entity_registry()

print((index.schemas.get("datasetKey")))
