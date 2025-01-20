import logging
from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph
)

logger = logging.getLogger("PUSH_ROLES_TO_ACRYL_PROC")

# Connect to the DataHub instance configured in your ~/.datahubenv file.
client: DataHubGraph = get_default_graph()

def push_roles_to_acryl(request):
    open_api_endpoint = f"{client.config.server}/openapi/v3/entity/role?async=false&systemMetadata=false" 
    logger.info(f"open_api_endpoint >> {open_api_endpoint}")
    
    response = client._post_generic(url=open_api_endpoint, payload_dict=request)
    print(response)


if __name__ == "__main__":
    urn = "urn:li:role:mlops_pii_role"
    request = [{
        "roleProperties": {
            "value": {
                "description": "Snowflake role under domain",
                "name": "MLOPS_PII_ROLE",
                "type": "PRODUCTION_PII_ROLES"
            }
        },
        "urn": "urn:li:role:mlops_pii_role",
    }]
    push_roles_to_acryl(request)
