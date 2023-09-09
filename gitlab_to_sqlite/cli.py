import click
import datetime
import pathlib
import textwrap
import os
import sqlite_utils
import time
import json
from gitlab_to_sqlite import utils


@click.group()
@click.version_option()
def cli():
    "Save data from GitLab to a SQLite database"


@cli.command()
@click.option(
    "-a",
    "--auth",
    type=click.Path(
        file_okay=True, dir_okay=False, allow_dash=False, path_type=pathlib.Path
    ),
    default="auth.json",
    help="Path to save tokens to, defaults to auth.json",
)
@click.option(
    "-h",
    "--host",
    type=str,
    default="gitlab.com",
    help="",
)
def auth(auth, host):
    "Save authentication credentials to a JSON file"
    click.echo("Create a GitLab personal user token and paste it here:")
    click.echo()
    personal_token = click.prompt("Personal token")
    if auth.exists():
        auth_data = json.load(auth.open())
    else:
        auth_data = {}
    auth_data["gitlab_personal_token"] = personal_token
    auth_data["gitlab_host"] = host
    with auth.open("w") as f:
        json.dump(auth_data, f, indent=4)
        f.write("\n")


@cli.command(name="projects")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("project", required=True)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True),
    default="auth.json",
    help="Path to auth.json token file",
)
def projects(db_path, project, auth):
    "Save projects"
    db = sqlite_utils.Database(db_path)
    token, host = load_config(auth)
    project = utils.fetch_project(project, token, host)
    utils.save_project(db, project)
    utils.ensure_db_shape(db)


@cli.command(name="merge-requests")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("project", required=True)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option(
    "--full",
    is_flag=True,
)
def merge_requests(db_path, project, auth, full):
    "Save merge requests"
    db = sqlite_utils.Database(db_path)
    token, host = load_config(auth)

    new = 0
    for merge_request in utils.fetch_merge_requests(
        project,
        token,
        host,
        None if full else utils.get_latest_merge_request_time(db, project),
    ):
        utils.save_merge_request(db, merge_request)
        new += 1

    utils.ensure_db_shape(db)
    click.echo(f"Saved/updated {new} merge requests")


@cli.command(name="pipelines")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("project", required=True)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option(
    "--full",
    is_flag=True,
)
def pipelines(db_path, project, auth, full):
    "Save pipelines"
    db = sqlite_utils.Database(db_path)
    token, host = load_config(auth)

    new = 0
    for pipeline in utils.fetch_pipelines(
        project,
        token,
        host,
        None if full else utils.get_latest_pipeline_time(db, project),
    ):
        utils.save_pipeline(db, pipeline, host)
        new += 1

    utils.ensure_db_shape(db)
    click.echo(f"Saved/updated {new} pipelines")


@cli.command(name="environments")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("project", required=True)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True),
    default="auth.json",
    help="Path to auth.json token file",
)
def environments(db_path, project, auth):
    db = sqlite_utils.Database(db_path)
    token, host = load_config(auth)

    new = 0
    for environment in utils.fetch_environments(
        project,
        token,
        host,
    ):
        utils.save_environment(db, environment)
        new += 1

    utils.ensure_db_shape(db)
    click.echo(f"Saved/updated {new} environments")


@cli.command(name="deployments")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("project", required=True)
@click.argument("environment", required=True)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True),
    default="auth.json",
    help="Path to auth.json token file",
)
def deployments(db_path, project, environment, auth):
    db = sqlite_utils.Database(db_path)
    token, host = load_config(auth)

    if (
        "deployments" in db.table_names()
        and "projects" in db.table_names()
        and "environments" in db.table_names()
    ):
        r = db.query(
            """
        SELECT
            max(d.updated_at) AS last_update
        FROM
            deployments d
            JOIN projects p ON d.project_id = p.id
            JOIN environments e ON d.environment_id = e.id
        WHERE
            p.full_path = ?
            AND e.name = ?
        """,
            [project, environment],
        )
        last_update = next(r)["last_update"]
    else:
        last_update = None

    new = 0
    for deployment in utils.fetch_deployments(
        project,
        environment,
        token,
        host,
        last_update,
    ):
        if utils.save_deployment(db, deployment) is not False:
            new += 1

    utils.ensure_db_shape(db)
    click.echo(f"Saved/updated {new} deployments")


def load_config(auth):
    try:
        data = json.load(open(auth))
        token = data["gitlab_personal_token"]
        host = data["gitlab_host"]
    except (KeyError, FileNotFoundError):
        token = None
        host = None
    if token is None:
        # Fallback to GITLAB_TOKEN environment variable
        token = os.environ.get("GITLAB_TOKEN")
    if host is None:
        # Fallback to GITLAB_HOST environment variable
        token = os.environ.get("GITLAB_HOST")
    return token, host
