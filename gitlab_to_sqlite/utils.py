from graphql import DocumentNode
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from sqlite_utils import Database


def get_client(host: str, token: str) -> Client:
    transport = AIOHTTPTransport(
        url=f"https://{host}/api/graphql",
        headers={"Authorization": f"Bearer {token}"},
    )
    return Client(transport=transport, fetch_schema_from_transport=True)


project_query = gql(
    """
query project ($project: ID!) {
  project(fullPath: $project) {
    id
    group {
      id
    }
    name
    path
    fullPath
  }
}
"""
)


def fetch_project(project: str, token: str, host: str) -> dict:
    client = get_client(host, token)
    return client.execute(project_query, variable_values={"project": project})[
        "project"
    ]


def save_project(db: Database, project: dict) -> None:
    data = {
        "id": project["id"].split("/")[-1],
        "group_id": project["group"]["id"].split("/")[-1],
        "name": project["name"],
        "path": project["path"],
        "full_path": project["fullPath"],
    }

    db["project"].insert(
        data,
        pk="id",
        alter=True,
        replace=True,
        columns={
            "id": int,
            "group_id": int,
            "name": str,
            "path": str,
            "full_path": str,
        },
    )


pipelines_query = gql(
    """
query pipelines ($project: ID!, $after: String, $updated_after: Time) {
  project(fullPath: $project) {
    pipelines(first: 100, after: $after, updatedAfter: $updated_after) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        createdAt
        updatedAt
        status
        duration
        project {
          id
        }

        jobs {
          nodes {
            id
            name
            createdAt
            queuedAt
            scheduledAt
            startedAt
            finishedAt
            manualJob
            stage {
              name
            }
            status
            queuedDuration
            duration
          }
        }
      }
    }
  }
}
  """
)


def fetch_pipelines(
    project: str, token: str, host: str, updated_or_created_after: str | None
) -> list[dict]:
    client = get_client(host, token)
    for pipeline in paginate(
        client,
        pipelines_query,
        "pipelines",
        project=project,
        updated_after=updated_or_created_after,
    ):
        yield pipeline


def save_pipeline(db: Database, pipeline: dict) -> None:
    data = {
        "id": pipeline["id"].split("/")[-1],
        "project_id": pipeline["project"]["id"].split("/")[-1],
        "created_at": pipeline["createdAt"],
        "updated_at": pipeline["updatedAt"],
        "status": pipeline["status"],
        "duration": pipeline["duration"],
    }

    db["pipelines"].insert(
        data,
        pk="id",
        alter=True,
        replace=True,
        columns={
            "id": int,
            "project_id": int,
            "created_at": str,
            "updated_at": str,
            "status": str,
            "duration": int,
        },
    )

    for job in pipeline["jobs"]["nodes"]:
        job_data = {
            "id": job["id"].split("/")[-1],
            "name": job["name"],
            "stage_name": job["stage"]["name"],
            "pipeline_id": data["id"],
            "project_id": data["project_id"],
            "created_at": job["createdAt"],
            "queued_at": job["queuedAt"],
            "scheduled_at": job["scheduledAt"],
            "started_at": job["startedAt"],
            "finished_at": job["finishedAt"],
            "manual": job["manualJob"],
            "status": job["status"],
            "queued_duration": job["queuedDuration"],
            "duration": job["duration"],
        }
        db["jobs"].insert(
            job_data,
            pk="id",
            alter=True,
            replace=True,
            columns={
                "id": int,
                "name": str,
                "stage_name": str,
                "pipeline_id": int,
                "project_id": int,
                "created_at": str,
                "queued_at": str,
                "scheduled_at": str,
                "started_at": str,
                "finished_at": str,
                "manual": bool,
                "status": str,
                "queued_duration": int,
                "duration": int,
            },
        )


def get_latest_pipeline_time(db: Database, project: str) -> str | None:
    result = db.query(
        """
        select id from project where full_path = ?""",
        [project],
    )
    project_id = next(result)["id"]

    if db["pipelines"].exists():
        result = db.query(
            """
            select max(created_at) as created, max(updated_at) as updated from pipelines where project_id = ?""",
            [project_id],
        )
        row = next(result)
        if row["created"] and row["updated"]:
            return max(row["created"], row["updated"])

    return None


def paginate(client: Client, query: DocumentNode, node: str, **args):
    has_next_page = True
    after_cursor = None
    while has_next_page:
        result = client.execute(query, variable_values={**args, "after": after_cursor})
        yield from result["project"][node]["nodes"]

        has_next_page = result["project"][node]["pageInfo"]["hasNextPage"]
        after_cursor = result["project"][node]["pageInfo"]["endCursor"]
