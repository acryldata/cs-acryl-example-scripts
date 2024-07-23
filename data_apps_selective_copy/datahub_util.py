import requests
import datetime
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
import click
import os
from typing import Optional

DATAHUB_GMS_TOKEN_ENV = "DATAHUB_GMS_TOKEN"
FRONTEND_URL_ENV = "DATAHUB_FRONTEND_URL"
GMS_URL_ENV = "DATAHUB_GMS_URL"

def get_gms_token_env() -> str:
    return os.getenv(DATAHUB_GMS_TOKEN_ENV)


def set_correct_env(
    url: str, token: Optional[str] = None, gms_url: Optional[str] = None
):
    os.environ[FRONTEND_URL_ENV] = url
    if GMS_URL_ENV in os.environ:
        del os.environ[GMS_URL_ENV]
    if gms_url is None:
        os.environ[GMS_URL_ENV] = get_gms_url()
    else:
        os.environ[GMS_URL_ENV] = gms_url
    if token is not None:
        os.environ[DATAHUB_GMS_TOKEN_ENV] = token


def get_frontend_url() -> str:
    return os.getenv(FRONTEND_URL_ENV)


def get_gms_url():
    return os.getenv(GMS_URL_ENV, f"{get_frontend_url()}/gms")


def _get_graphql_url():
    return f"{get_frontend_url()}/api/graphql"

def _graphql_query(graph: DataHubGraph, query, vars):
    response = graph._post_generic(
        url=_get_graphql_url(),
        payload_dict={"query": query, "variables": vars},
    )
    if "errors" in response:
        click.secho(f"Error: {response['errors']}", fg="red")
    return response

def get_session_login_as(username: str, password: str, url: str) -> requests.Session:
    session = requests.Session()
    headers = {
        "Content-Type": "application/json",
    }
    data = '{"username":"' + username + '", "password":"' + password + '"}'
    response = session.post(f"{url}/logIn", headers=headers, data=data)
    session.url = url
    response.raise_for_status()
    return session


def generate_access_token(
    url: str,
    session: requests.Session,
    validity: str,
    actorUrn: str = "urn:li:corpuser:admin",
) -> str:
    now = datetime.datetime.now()
    timestamp = now.astimezone().isoformat()
    name = f"Acryl Data Support Token generated at {timestamp}"
    json = {
        "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
            createAccessToken(input: $input) {
              accessToken
              metadata {
                id
                actorUrn
                ownerUrn
                name
                description
              }
            }
        }""",
        "variables": {
            "input": {
                "type": "PERSONAL",
                "actorUrn": actorUrn,
                "duration": validity,
                "name": name,
            }
        },
    }
    response = session.post(f"{url}/api/v2/graphql", json=json)
    response.raise_for_status()
    return name, response.json().get("data", {}).get("createAccessToken", {}).get(
        "accessToken", None
    )


def delete_access_token(url: str, session: requests.Session, token_id: str):
    json = {
        "query": """mutation revokeAccessToken($tokenId: String!) {\n
            revokeAccessToken(tokenId: $tokenId)
        }""",
        "variables": {"tokenId": token_id},
    }

    response = session.post(f"{url}/api/v2/graphql", json=json)
    response.raise_for_status()
    return response.json()


def raise_incident(
    graph: DataHubGraph, type_in: str, urn: str, title: str, description: str
):
    _graphql_query(
        graph,
        """
    mutation raiseIncident($input: RaiseIncidentInput!) {
        raiseIncident(input: $input)
    }""",
        vars={
            "input": {
                "type": type_in,
                "title": title,
                "description": description,
                "resourceUrn": urn,
            }
        },
    )

def get_graph():
    return DataHubGraph(
        DatahubClientConfig(server=get_gms_url(), token=get_gms_token_env())
    )