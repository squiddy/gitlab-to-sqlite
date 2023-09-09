from setuptools import setup
import pathlib

VERSION = "0.0.1"


def get_long_description():
    file = pathlib.Path(__file__).parent / "README.md"
    with file.open(encoding="utf8") as fp:
        return fp.read()


setup(
    name="gitlab-to-sqlite",
    description="Save data from GitLab to a SQLite database",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Reiner Gerecke",
    url="https://github.com/squiddy/gitlab-to-sqlite",
    license="MIT",
    version=VERSION,
    packages=["gitlab_to_sqlite"],
    entry_points="""
        [console_scripts]
        gitlab-to-sqlite=gitlab_to_sqlite.cli:cli
    """,
    install_requires=["sqlite-utils>=2.7.2", "gql[all]", "python-gitlab"],
    extras_require={"test": ["pytest", "black"]},
    tests_require=["gitlab-to-sqlite[test]"],
)
