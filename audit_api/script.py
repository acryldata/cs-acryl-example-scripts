# ABOUTME: CLI tool for downloading audit logs from DataHub using the audit events search API
# ABOUTME: Supports filtering by event types, entity types, aspect types, and actors with configurable output formats

import logging
import json
import csv
from datetime import datetime, timedelta
from typing import Optional, List
import click
from rich.logging import RichHandler
from rich.console import Console

from datahub.ingestion.graph.client import (
    DataHubGraph,
    get_default_graph
)

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()]
)

logger = logging.getLogger("rich")
console = Console()


def get_timestamp_ms(date_str: str) -> int:
    """Convert date string to timestamp in milliseconds."""
    dt_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return int(dt_obj.timestamp() * 1000)


def download_audit_logs(
    graph: DataHubGraph,
    output_file: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    event_types: Optional[List[str]] = None,
    entity_types: Optional[List[str]] = None,
    aspect_types: Optional[List[str]] = None,
    actor_urns: Optional[List[str]] = None,
    page_size: int = 100,
    include_raw: bool = True,
    output_format: str = "json"
) -> int:
    """
    Download audit logs from DataHub.

    Returns the total number of events downloaded.
    """
    gms_host = graph._gms_server
    session = graph._session
    url = f"{gms_host}/openapi/v1/events/audit/search"

    # Build query parameters
    params = {
        "size": page_size,
        "includeRaw": str(include_raw).lower()
    }

    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    # Build request body for filters
    filters = {}
    if event_types:
        filters["eventTypes"] = event_types
    if entity_types:
        filters["entityTypes"] = entity_types
    if aspect_types:
        filters["aspectTypes"] = aspect_types
    if actor_urns:
        filters["actorUrns"] = actor_urns

    headers = {
        "Content-Type": "application/json",
        "accept": "application/json",
    }

    total_downloaded = 0
    scroll_id = None
    first_page = True

    # Initialize output file based on format
    if output_format == "json":
        with open(output_file, "w") as f:
            f.write("[")
    elif output_format == "jsonl":
        with open(output_file, "w") as f:
            pass
    elif output_format == "csv":
        pass

    csv_writer = None
    csv_file = None
    csv_headers_written = False

    try:
        if output_format == "csv":
            csv_file = open(output_file, "w", newline="")
            csv_writer = csv.writer(csv_file)

        while first_page or scroll_id:
            first_page = False

            if scroll_id:
                params["scrollId"] = scroll_id

            response = session.post(
                url,
                params=params,
                json=filters if filters else {},
                headers=headers
            )
            response.raise_for_status()

            data = response.json()
            events = data.get("usageEvents", [])
            scroll_id = data.get("nextScrollId")

            if not events:
                break

            # Write events based on format
            if output_format == "json":
                with open(output_file, "a") as f:
                    for i, event in enumerate(events):
                        if total_downloaded > 0 or i > 0:
                            f.write(",\n")
                        json.dump(event, f, indent=2)

            elif output_format == "jsonl":
                with open(output_file, "a") as f:
                    for event in events:
                        json.dump(event, f)
                        f.write("\n")

            elif output_format == "csv":
                for event in events:
                    flat_event = {
                        "eventType": event.get("eventType"),
                        "timestamp": event.get("timestamp"),
                        "actorUrn": event.get("actorUrn"),
                        "sourceIP": event.get("sourceIP"),
                        "eventSource": event.get("eventSource"),
                        "userAgent": event.get("userAgent"),
                    }

                    if csv_writer is not None:
                        if not csv_headers_written:
                            csv_writer.writerow(flat_event.keys())
                            csv_headers_written = True

                        csv_writer.writerow(flat_event.values())

            total_downloaded += len(events)
            logger.info(f"Downloaded {total_downloaded} events...")

        if output_format == "json":
            with open(output_file, "a") as f:
                f.write("\n]")

    finally:
        if csv_file:
            csv_file.close()

    return total_downloaded


@click.command()
@click.option(
    "--output",
    "-o",
    default="audit_logs.json",
    help="Output file path",
    show_default=True
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "jsonl", "csv"], case_sensitive=False),
    default="json",
    help="Output format",
    show_default=True
)
@click.option(
    "--start-date",
    help="Start date in YYYY-MM-DD HH:MM:SS format (default: 24 hours ago)"
)
@click.option(
    "--end-date",
    help="End date in YYYY-MM-DD HH:MM:SS format (default: now)"
)
@click.option(
    "--event-types",
    help="Comma-separated list of event types to filter"
)
@click.option(
    "--entity-types",
    help="Comma-separated list of entity types to filter"
)
@click.option(
    "--aspect-types",
    help="Comma-separated list of aspect types to filter"
)
@click.option(
    "--actor-urns",
    help="Comma-separated list of actor URNs to filter"
)
@click.option(
    "--page-size",
    default=100,
    help="Number of events per page",
    show_default=True
)
@click.option(
    "--include-raw/--no-include-raw",
    default=True,
    help="Include raw event data",
    show_default=True
)
def main(
    output: str,
    output_format: str,
    start_date: Optional[str],
    end_date: Optional[str],
    event_types: Optional[str],
    entity_types: Optional[str],
    aspect_types: Optional[str],
    actor_urns: Optional[str],
    page_size: int,
    include_raw: bool
):
    """
    Download audit logs from DataHub.

    Examples:

    \b
    # Download last 24 hours of logs
    python script.py

    \b
    # Download logs for specific date range
    python script.py --start-date "2025-01-01 00:00:00" --end-date "2025-01-02 00:00:00"

    \b
    # Download logs filtered by event type
    python script.py --event-types "LogInEvent,FailedLogInEvent"

    \b
    # Download logs for specific user
    python script.py --actor-urns "urn:li:corpuser:john.doe"

    \b
    # Export to CSV
    python script.py --format csv --output audit_logs.csv
    """
    console.print("[bold blue]DataHub Audit Log Downloader[/bold blue]")

    start_time = None
    end_time = None

    if start_date:
        try:
            start_time = get_timestamp_ms(start_date)
        except ValueError:
            console.print("[bold red]Error:[/bold red] Invalid start date format. Use YYYY-MM-DD HH:MM:SS")
            return
    else:
        start_time = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)

    if end_date:
        try:
            end_time = get_timestamp_ms(end_date)
        except ValueError:
            console.print("[bold red]Error:[/bold red] Invalid end date format. Use YYYY-MM-DD HH:MM:SS")
            return
    else:
        end_time = int(datetime.now().timestamp() * 1000)

    event_types_list = event_types.split(",") if event_types else None
    entity_types_list = entity_types.split(",") if entity_types else None
    aspect_types_list = aspect_types.split(",") if aspect_types else None
    actor_urns_list = actor_urns.split(",") if actor_urns else None

    console.print(f"\n[cyan]Configuration:[/cyan]")
    console.print(f"  Output file: {output}")
    console.print(f"  Output format: {output_format}")
    console.print(f"  Start time: {datetime.fromtimestamp(start_time / 1000)}")
    console.print(f"  End time: {datetime.fromtimestamp(end_time / 1000)}")
    if event_types_list:
        console.print(f"  Event types: {', '.join(event_types_list)}")
    if entity_types_list:
        console.print(f"  Entity types: {', '.join(entity_types_list)}")
    if aspect_types_list:
        console.print(f"  Aspect types: {', '.join(aspect_types_list)}")
    if actor_urns_list:
        console.print(f"  Actor URNs: {', '.join(actor_urns_list)}")
    console.print()

    try:
        graph = get_default_graph()
        console.print(f"[green]Connected to DataHub:[/green] {graph._gms_server}")
    except Exception as e:
        console.print(f"[bold red]Error connecting to DataHub:[/bold red] {e}")
        return

    try:
        total = download_audit_logs(
            graph=graph,
            output_file=output,
            start_time=start_time,
            end_time=end_time,
            event_types=event_types_list,
            entity_types=entity_types_list,
            aspect_types=aspect_types_list,
            actor_urns=actor_urns_list,
            page_size=page_size,
            include_raw=include_raw,
            output_format=output_format
        )

        console.print(f"\n[bold green]Success![/bold green] Downloaded {total} events to {output}")

    except Exception as e:
        console.print(f"\n[bold red]Error downloading logs:[/bold red] {e}")
        logger.exception("Full error details:")
        return


if __name__ == "__main__":
    main()