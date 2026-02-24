# ABOUTME: Checks the number of Kafka partitions in a DataHub instance by querying consumer offset endpoints
# ABOUTME: Supports console (pretty table) and json output modes

import json
import logging
import sys

import click
from rich.console import Console
from rich.table import Table

from datahub.ingestion.graph.client import DataHubGraph, get_default_graph

logging.basicConfig(level=logging.WARNING)

console = Console()

ENDPOINTS = {
    "mcl": "openapi/operations/kafka/mcl/consumer/offsets",
    "mcp": "openapi/operations/kafka/mcp/consumer/offsets",
}


def get_base_url(graph: DataHubGraph) -> str:
    """Derive the base instance URL by stripping the /gms suffix."""
    gms_server = graph._gms_server.rstrip("/")
    if gms_server.endswith("/gms"):
        return gms_server[: -len("/gms")]
    return gms_server


def fetch_consumer_offsets(graph: DataHubGraph, path: str) -> dict:
    """Fetch consumer offset details for a Kafka consumer group."""
    base_url = get_base_url(graph)
    session = graph._session
    url = f"{base_url}/{path}"
    params = {"skipCache": "true", "detailed": "true"}

    response = session.get(url, params=params)
    response.raise_for_status()
    return response.json()


def parse_offsets(offsets_response: dict) -> list[dict]:
    """Parse consumer offsets response into per-topic partition summaries."""
    consumer_group = offsets_response.get("consumerGroupId")
    topics = offsets_response.get("topics", {})
    results = []
    for topic_name, topic_data in topics.items():
        partitions = topic_data.get("partitions", {})
        metrics = topic_data.get("metrics", {})
        results.append(
            {
                "consumerGroup": consumer_group,
                "topic": topic_name,
                "partitions": len(partitions),
                "totalLag": metrics.get("totalLag"),
                "maxLag": metrics.get("maxLag"),
            }
        )
    return results


def collect_data(graph: DataHubGraph) -> dict:
    """Fetch and parse offsets for all endpoints."""
    output = {}
    for label, path in ENDPOINTS.items():
        try:
            data = fetch_consumer_offsets(graph, path)
            output[label] = parse_offsets(data)
        except Exception as e:
            output[label] = {"error": str(e)}
    return output


def print_console(graph: DataHubGraph, data: dict):
    """Pretty-print results as a Rich table."""
    instance = get_base_url(graph)

    table = Table(title=instance, show_lines=False)
    table.add_column("Type", style="bold")
    table.add_column("Consumer Group")
    table.add_column("Topic")
    table.add_column("Partitions", justify="right", style="green")
    table.add_column("Total Lag", justify="right")
    table.add_column("Max Lag", justify="right")

    for label, value in data.items():
        if isinstance(value, dict) and "error" in value:
            table.add_row(
                label.upper(), f"[red]ERROR: {value['error']}[/red]", "", "", "", ""
            )
            continue
        if not value:
            table.add_row(label.upper(), "-", "-", "-", "-", "-")
            continue
        for entry in value:
            table.add_row(
                label.upper(),
                entry["consumerGroup"] or "-",
                entry["topic"],
                str(entry["partitions"]),
                str(entry["totalLag"]),
                str(entry["maxLag"]),
            )

    console.print(table)


def print_json(data: dict):
    """Emit data as a single JSON object to stdout."""
    json.dump(data, sys.stdout)


@click.command()
@click.option(
    "--output",
    "-o",
    type=click.Choice(["console", "json"], case_sensitive=False),
    default="console",
    show_default=True,
    help="Output format",
)
def main(output: str):
    """Check the number of Kafka partitions in a DataHub instance.

    \b
    Examples:
        python script.py
        python script.py -o json
    """
    try:
        graph = get_default_graph()
    except Exception as e:
        if output == "json":
            json.dump({"error": str(e)}, sys.stdout)
        else:
            console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    data = collect_data(graph)

    if output == "json":
        print_json(data)
    else:
        print_console(graph, data)


if __name__ == "__main__":
    main()
