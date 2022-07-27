# gitlab-to-sqlite

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/squiddy/gitlab-to-sqlite/blob/main/LICENSE)

Save data from GitLab to a SQLite database.

## Attribution

The overall structure and CLI is taken from
https://github.com/dogsheep/github-to-sqlite/.

- [How to install](#how-to-install)
- [Authentication](#authentication)
- [Using custom gitlab instance](#using-custom-gitlab-instance)
- [Fetching issues for a repository](#fetching-issues-for-a-repository)

## How to install

    $ pip install gitlab-to-sqlite

## Authentication

Create a GitLab personal access token: https://gitlab.com/-/profile/personal_access_tokens

Run this command and paste in your new token:

    $ gitlab-to-sqlite auth

This will create a file called auth.json in your current directory containing
the required value. To save the file at a different path or filename, use the
--auth=myauth.json option.

As an alternative to using an auth.json file you can add your access token to an
environment variable called GITLAB_TOKEN.

## Using custom gitlab instance

When running ``auth`` you may specify an optional ``--host`` parameter pointing
to a custom instance.

    $ gitlab-to-sqlite auth --host gitlab.internal

## Fetching projects

The `projects` command retrieves a single project.

    $ gitlab-to-sqlite projects gitlab.db group/project-name

## Fetching pipelines

The `pipelines` command retrieves updated or created pipelines with their
corresponding jobs.

    $ gitlab-to-sqlite pipelines gitlab.db group/project-name

This command can be run regularly. Based on the most recent created or updated
pipeline it only fetches changes that happened afterwards.