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
def pipelines(db_path, project, auth):
    "Save pipelines"
    db = sqlite_utils.Database(db_path)
    token, host = load_config(auth)
    latest = utils.get_latest_pipeline_time(db, project)

    new = 0
    for pipeline in utils.fetch_pipelines(project, token, host, latest):
        utils.save_pipeline(db, pipeline)
        new += 1

    click.echo(f"Saved/updated {new} pipelines")


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
