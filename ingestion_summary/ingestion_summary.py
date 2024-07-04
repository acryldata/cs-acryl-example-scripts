import datetime
import json
from dataclasses import dataclass, field
from typing import Dict, List, Any
import os

import logging

logger = logging.getLogger(__name__)


def get_now_utc_datetime() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def x_days_ago_datetime(days: int) -> datetime.datetime:
    return get_now_utc_datetime() - datetime.timedelta(days=days)

def x_days_ago_millis(days: int) -> int:
    return int(x_days_ago_datetime(days).timestamp() * 1000)


def write_result(dir_name: str, file_name: str, content: Any):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    with open(f"{dir_name}/{file_name}", "w") as f:
        json.dump(content, f, indent=4)


@dataclass
class Execution:
    urn: str = field(default=None)
    status: bool = field(default=None)
    startTime: datetime.datetime = field(default=None)
    report: dict = field(default_factory=dict)

    def to_serializable_dict(self):
        return {
            "urn": self.urn,
            "report": self.report,
            "status": self.status,
        }


@dataclass
class IngestionSource:
    urn: str = field(default=None)
    name: str = field(default=None)
    version: str = field(default=None)
    needs_update: bool = field(default=False)
    type: str = field(default=None)
    recipe: str = field(default=None)
    last_execution_start_time: str = field(default=None)
    executions: List[Execution] = field(default_factory=list)

    def to_serializable_dict(self):
        return {
            "urn": self.urn,
            "name": self.name,
            "version": self.version,
            "needs_update": self.needs_update,
            "success_count": sum(1 for i in self.executions if i.status == "SUCCESS"),
            "failure_count": sum(1 for i in self.executions if i.status == "FAILURE"),
            "pending_count": sum(1 for i in self.executions if i.status == "PENDING"),
            "type": self.type,
            "recipe": self.recipe,
            "executions": [i.to_serializable_dict() for i in self.executions],
        }


@dataclass
class IngestionSummary:
    run_at: datetime.datetime = field(default=None)
    total: int = field(default=None)
    top_records_written: Dict = field(default_factory=dict)
    ingestion_sources: List[IngestionSource] = field(default_factory=list)

    def to_serializable_dict(self):
        valid_sources = [
            i.to_serializable_dict()
            for i in self.ingestion_sources
            if i.last_execution_start_time
        ]
        success_exec = sum(i["success_count"] for i in valid_sources)
        failure_exec = sum(i["failure_count"] for i in valid_sources)
        pending_exec = sum(i["pending_count"] for i in valid_sources)
        needs_update = sum(1 for i in valid_sources if i["needs_update"])
        return {
            "run_at": self.run_at.astimezone().isoformat(),
            "total": len(valid_sources),
            "success_exec": success_exec,
            "failure_exec": failure_exec,
            "pending_exec": pending_exec,
            "needs_update": needs_update,
            "ingestion_sources": valid_sources,
            "top_records_written": self.top_records_written,
        }
    
import requests

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

def list_ingestions(
    url: str, session: requests.Session, start: int = 0, count: int = 10
):
    json = {
        "query": """query listIngestionSources($input: ListIngestionSourcesInput!) {
  listIngestionSources(input: $input) {
    start
    count
    total
    ingestionSources {
      urn
      type
      name
      config {
        recipe
        version
      }
      executions(start: 0, count: 10) {
        executionRequests {
          urn
          result {
            status
            startTimeMs
            structuredReport {
              serializedValue
            }
          }
        }
      }
    }
  }
}
""",
        "variables": {"input": {"start": start, "count": count, "query": "*"}},
    }
    response = session.post(f"{url}/api/v2/graphql", json=json)
    return response.json().get("data", {}).get("listIngestionSources", {})


def get_ingestion_summary(url: str, user: str, password: str):
    days_ago_millis = x_days_ago_millis(7)
    result = IngestionSummary()
    result.run_at = datetime.datetime.now()
    # random large number will be replaced by actual total
    result.total = 10000

    session = get_session_login_as(
        url=url, username=user, password=password
    )
    url = session.url
    cur = 0
    while cur < result.total:
        ingestions_list = list_ingestions(
            url=url, session=session, start=cur
        )
        result.total = ingestions_list.get("total")
        for jj, ingestion_source_in in enumerate(
            ingestions_list.get("ingestionSources")
        ):
            cur += 1
            ingestion_source = IngestionSource()
            ingestion_source.urn = ingestion_source_in.get("urn")
            result.ingestion_sources.append(ingestion_source)
            ingestion_source.name = ingestion_source_in.get("name", None)
            ingestion_config = ingestion_source_in.get("config", {})
            ingestion_source.version = ingestion_config.get("version", None)
            recipe = ingestion_config.get("recipe")
            if recipe == "":
                logger.error(f"No recipe found for {ingestion_source.urn}")
                continue
            ingestion_source.recipe = json.loads(recipe)
            ingestion_source.type = ingestion_source.recipe.get("source", {}).get(
                "type", {}
            )
            for i, execution_requests in enumerate(
                ingestion_source_in.get("executions", {}).get("executionRequests", {})
            ):
                logger.info(
                    f"Processing {i} execution request for {jj} ingestion source"
                )
                execution_res = Execution()
                tmp_result = execution_requests.get("result", {})
                if tmp_result is None:
                    logger.error(
                        f"No result found for {ingestion_source.type} {ingestion_source.urn}"
                    )
                    continue
                execution_res.startTime = tmp_result.get("startTimeMs")
                if execution_res.startTime < days_ago_millis:
                    # we don't want to read in older datetimes
                    continue
                execution_res.urn = execution_requests.get("urn")
                ingestion_source.executions.append(execution_res)
                execution_res.status = tmp_result.get("status")
                structured_report = tmp_result.get("structuredReport", {})
                if structured_report is not None:
                    try:
                        serialized_value = json.loads(
                            structured_report.get("serializedValue")
                        )
                    except:
                        logger.error(
                            f"Failed to parse structured report for {ingestion_source.urn}"
                        )
                    total_records_written = 0
                    total_records_written_new = (
                        serialized_value.get("sink", {})
                        .get("report", {})
                        .get("total_records_written", 0)
                    )
                    if ingestion_source.urn in result.top_records_written:
                        total_records_written = result.top_records_written.get(
                            ingestion_source.urn
                        )
                    if total_records_written_new > total_records_written:
                        result.top_records_written[
                            ingestion_source.urn
                        ] = total_records_written_new
                    execution_res.report = serialized_value
            if ingestion_source.executions:
                ingestion_source.last_execution_start_time = max(
                    i.startTime for i in ingestion_source.executions
                )
    write_result(
        dir_name=".",
        file_name=f"ingestion_summary.json",
        content=result.to_serializable_dict(),
    )


if __name__ == "__main__":
    get_ingestion_summary(url="URL", user="admin", password="PASSWORD")