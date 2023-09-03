import datetime
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

    db["projects"].insert(
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
        startedAt
        finishedAt
        status
        duration
        project {
          id
        }
        commit {
          sha
        }
        ref

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
            webPath
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
    yield from paginate(
        client,
        pipelines_query,
        "pipelines",
        project=project,
        updated_after=updated_or_created_after,
    )


def save_pipeline(db: Database, pipeline: dict, host: str) -> None:
    if "projects" not in db.table_names():
        db["projects"].create({"id": int}, pk="id")

    data = {
        "id": pipeline["id"].split("/")[-1],
        "project_id": pipeline["project"]["id"].split("/")[-1],
        "created_at": pipeline["createdAt"],
        "updated_at": pipeline["updatedAt"],
        "started_at": pipeline["startedAt"],
        "finished_at": pipeline["finishedAt"],
        "status": pipeline["status"],
        "duration": pipeline["duration"],
        "commit_sha": pipeline["commit"]["sha"],
        "ref": pipeline["ref"],
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
            "started_at": str,
            "finished_at": str,
            "status": str,
            "duration": int,
            "commit_sha": str,
            "ref": str,
        },
        foreign_keys=[("project_id", "projects", "id")],
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
            "web_url": f"https://{host}{job['webPath']}"
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
                "web_url": str,
            },
            foreign_keys=[
                ("pipeline_id", "pipelines", "id"),
                ("project_id", "projects", "id"),
            ],
        )


def get_latest_pipeline_time(db: Database, project: str) -> str | None:
    result = db.query(
        """
        select id from projects where full_path = ?""",
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


merge_requests_query = gql(
    """
query merge_requests($project: ID!, $after: String, $updated_after: Time) {
  project(fullPath: $project) {
    mergeRequests(first: 100, after: $after, updatedAfter: $updated_after) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        webUrl
        targetBranch
        targetProjectId

        createdAt
        mergedAt
        updatedAt

        commitCount
        userDiscussionsCount
        userNotesCount
        diffStatsSummary {
          additions
          changes
          deletions
          fileCount
        }

        state
        title
        description

        headPipeline {
          id
        }
      }
    }
  }
}
  """
)


def fetch_merge_requests(
    project: str, token: str, host: str, updated_or_created_after: str | None
) -> list[dict]:
    client = get_client(host, token)
    yield from paginate(
        client,
        merge_requests_query,
        "mergeRequests",
        project=project,
        updated_after=updated_or_created_after,
    )


def save_merge_request(db: Database, merge_request: dict) -> None:
    if "pipelines" not in db.table_names():
        db["pipelines"].create({"id": int}, pk="id")
    if "projects" not in db.table_names():
        db["projects"].create({"id": int}, pk="id")

    data = {
        "id": merge_request["id"].split("/")[-1],
        "web_url": merge_request["webUrl"],
        "target_branch": merge_request["targetBranch"],
        "target_project_id": merge_request["targetProjectId"],

        "created_at": merge_request["createdAt"],
        "merged_at": merge_request["mergedAt"],
        "updated_at": merge_request["updatedAt"],

        "commit_count": merge_request["commitCount"],
        "user_discussions_count": merge_request["userDiscussionsCount"],
        "user_notes_count": merge_request["userNotesCount"],
        "diff_stats_additions": merge_request["diffStatsSummary"]["additions"],
        "diff_stats_changes": merge_request["diffStatsSummary"]["changes"],
        "diff_stats_deletions": merge_request["diffStatsSummary"]["deletions"],
        "diff_stats_file_count": merge_request["diffStatsSummary"]["fileCount"],

        "state": merge_request["state"],
        "title": merge_request["title"],
        "description": merge_request["description"],

        "head_pipeline_id": merge_request["headPipeline"]["id"].split("/")[-1]
        if merge_request["headPipeline"]
        else None,
    }

    db["pipelines"].upsert(
      {"id": data["head_pipeline_id"]}, pk="id"
    )

    db["projects"].upsert(
      {"id": data["target_project_id"]}, pk="id"
    )

    db["merge_requests"].upsert(
        data,
        pk="id",
        alter=True,
        columns={
            "id": int,
            "web_url": str,
            "target_branch": str,
            "target_project_id": int,

            "created_at": datetime.datetime,
            "merged_at": datetime.datetime,
            "updated_at": datetime.datetime,

            "commit_count": int,
            "user_discussions_count": int,
            "user_notes_count": int,
            "diff_stats_additions": int,
            "diff_stats_changes": int,
            "diff_stats_deletions": int,
            "diff_stats_file_count": int,

            "state": str,
            "title": str,
            "description": str,

            "head_pipeline_id": str,
        },
        foreign_keys=[
            ("head_pipeline_id", "pipelines", "id"),
            ("target_project_id", "projects", "id"),
        ],
    )


def get_latest_merge_request_time(db: Database, project: str) -> str | None:
    project = next(db["projects"].rows_where("full_path = ?", [project]))

    if db["merge_requests"].exists():
        result = db.query(
            """
            SELECT MAX(created_at) AS created, MAX(updated_at) AS updated
            FROM merge_requests 
            WHERE target_project_id = ?""",
            [project["id"]],
        )
        row = next(result)
        if row["created"] and row["updated"]:
            return max(row["created"], row["updated"])

    return None


def paginate(client: Client, query: DocumentNode, node: str, **args):
    has_next_page = True
    after_cursor = None
    while has_next_page:
        attempt = 0
        while True:
            try:
                result = client.execute(query, variable_values={**args, "after": after_cursor})
                break
            except Exception as e:
                attempt += 1
                if attempt > 4:
                    raise
                continue

        yield from result["project"][node]["nodes"]

        has_next_page = result["project"][node]["pageInfo"]["hasNextPage"]
        after_cursor = result["project"][node]["pageInfo"]["endCursor"]
