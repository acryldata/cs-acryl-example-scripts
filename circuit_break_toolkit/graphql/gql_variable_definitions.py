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
    variables = {"urn": entity_urn}
    return variables


def get_add_tag_vars(entity_urn: str, tag_urn: str) -> dict:
    variables = {
        "input": {
            "resourceUrn": entity_urn,
            "tagUrn": tag_urn,
        }
    }
    return variables


def get_remove_tag_vars(entity_urn: str, tag_urn: str) -> dict:
    variables = {
        "input": {
            "resourceUrn": entity_urn,
            "tagUrn": tag_urn,
        }
    }
    return variables


def get_assertion_details_vars(assertion_urn: str) -> dict:
    variables = {"assertionUrn": assertion_urn}
    return variables


def get_run_assertion_vars(assertion_urn: str) -> dict:
    variables = {"urn": assertion_urn}
    return variables
