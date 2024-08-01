def get_scroll_across_lineage_vars(
    entity_urn: str, direction: str, count: int, values: list = ["1", "2", "3+"]
) -> dict:
    variables = {
        "input": {
            "urn": entity_urn,
            "direction": direction,
            "query": "",
            "count": count,  # how many entities to pull at most
            "orFilters": [
                {
                    "and": [
                        {
                            "field": "degree",
                            "condition": "EQUAL",
                            "values": values,  # degree values to filter by
                            "negated": "false",
                        }
                    ]
                }
            ],
        }
    }
    return variables


def get_dataset_assertions_vars(entity_urn: str) -> dict:
    variables = {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:redshift,rs.dokken.prod.dokken_prod.wba_events.core_heartbeat,PROD)"
    }
    return variables


def get_add_tag_vars(entity_urn: str, tag_urn: str) -> dict:
    variables = {
        "input": {
            "resourceUrn": entity_urn,
            "tagUrn": tag_urn,
        }
    }
    return variables
