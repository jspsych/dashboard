"""
Pipeline-layer tests for commit ingestion.

fetch_api is monkeypatched to return synthetic REST-shaped commit payloads
(matching the real GitHub API, including a null `author` account), so these
tests exercise _process_commit + fetch_and_store_commits with no network.
"""

import sqlite3

import pytest
from conftest import MAIN

from src import data_pipeline
from src.data_pipeline import GitHubDataPipeline


def _raw_commit(sha, login, parents=1, message="Add feature\n\nlong body here",
                name="Dev Name", email="dev@example.com",
                authored="2024-01-01T00:00:00Z", committed="2024-01-01T01:00:00Z"):
    """Build a raw commit payload matching the GitHub REST commits API shape.

    `login=None` mimics a commit whose email isn't linked to a GitHub account
    (the top-level `author` field is null). `parents` sets the parent count so
    merge-commit detection (>1) can be exercised.
    """
    return {
        "sha": sha,
        "commit": {
            "author": {"name": name, "email": email, "date": authored},
            "committer": {"name": name, "email": email, "date": committed},
            "message": message,
        },
        "author": {"login": login} if login else None,
        "parents": [{"sha": f"p{i}"} for i in range(parents)],
    }


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    """A GitHubDataPipeline against a temp DB with a fake token."""
    monkeypatch.setattr(data_pipeline, "GITHUB_TOKEN", "fake-token")
    return GitHubDataPipeline(repo=MAIN, db_path=str(tmp_path / "p.db"))


def _rows(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return {r["sha"]: dict(r) for r in conn.execute("SELECT * FROM commits")}
    finally:
        conn.close()


def test_process_commit_maps_fields(pipeline):
    raw = _raw_commit("s1", "coredev")
    processed = pipeline._process_commit(raw)
    assert processed["repo"] == MAIN
    assert processed["sha"] == "s1"
    assert processed["author_login"] == "coredev"
    assert processed["author_name"] == "Dev Name"
    assert processed["author_email"] == "dev@example.com"
    assert processed["authored_at"] == "2024-01-01T00:00:00Z"
    assert processed["committed_at"] == "2024-01-01T01:00:00Z"
    # Only the first line of the message is kept.
    assert processed["message_headline"] == "Add feature"


def test_process_commit_null_author_login(pipeline):
    processed = pipeline._process_commit(_raw_commit("s2", None))
    assert processed["author_login"] is None
    # git author metadata is still present even without a linked account.
    assert processed["author_name"] == "Dev Name"


def test_process_commit_merge_derivation(pipeline):
    assert pipeline._process_commit(_raw_commit("s3", "coredev", parents=1))["is_merge_commit"] is False
    assert pipeline._process_commit(_raw_commit("s4", "coredev", parents=2))["is_merge_commit"] is True
    # A root commit (no parents) is not a merge.
    assert pipeline._process_commit(_raw_commit("s5", "coredev", parents=0))["is_merge_commit"] is False


def test_fetch_and_store_commits_processes_payloads(pipeline, monkeypatch):
    payloads = [
        _raw_commit("a", "coredev", parents=1),
        _raw_commit("b", None, parents=1),          # unlinked author
        _raw_commit("c", "alice", parents=2),        # merge commit
    ]
    monkeypatch.setattr(data_pipeline, "fetch_api", lambda repo, endpoint, token: payloads)

    stored = pipeline.fetch_and_store_commits()
    assert stored == 3

    rows = _rows(pipeline.db.db_path)
    assert set(rows) == {"a", "b", "c"}
    assert rows["b"]["author_login"] is None
    assert rows["c"]["is_merge_commit"] == 1
    assert rows["a"]["is_merge_commit"] == 0
    # Sync metadata updated for the repo.
    assert pipeline.db.get_sync_time_value("commit", MAIN) is not None
    assert pipeline.db.get_metadata(f"total_commits_tracked:{MAIN}") == "3"


def test_fetch_and_store_commits_empty(pipeline, monkeypatch):
    monkeypatch.setattr(data_pipeline, "fetch_api", lambda repo, endpoint, token: None)
    assert pipeline.fetch_and_store_commits() == 0


def test_fetch_and_store_commits_since_passes_filter(pipeline, monkeypatch):
    seen = {}

    def _capture(repo, endpoint, token):
        seen["endpoint"] = endpoint
        return [_raw_commit("z", "coredev")]

    monkeypatch.setattr(data_pipeline, "fetch_api", _capture)
    pipeline.fetch_and_store_commits_since("2024-06-01T00:00:00")
    assert "since=2024-06-01T00:00:00" in seen["endpoint"]
