"""
Database-layer tests: schema creation, upsert idempotency, the
get_activity_timeline placeholder regression (bug 1), and the legacy
no-repo-column -> repo-aware auto-migration.
"""

import sqlite3
from datetime import datetime, timezone

from conftest import MAIN, _pr

from src import repos
from src.database import DatabaseManager
from src.models import DatabaseSchema

EXPECTED_TABLES = {
    "pull_requests", "issues", "reviews", "comments", "releases",
    "discussions", "discussion_comments", "metadata",
}


def _table_names(path):
    conn = sqlite3.connect(str(path))
    try:
        return {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()


def _columns(path, table):
    conn = sqlite3.connect(str(path))
    try:
        return [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    finally:
        conn.close()


def test_schema_creates_all_tables(tmp_path):
    path = tmp_path / "fresh.db"
    DatabaseManager(str(path))
    assert EXPECTED_TABLES <= _table_names(path)


def test_schema_has_repo_column_on_repo_tables(tmp_path):
    path = tmp_path / "fresh.db"
    DatabaseManager(str(path))
    for table in ("pull_requests", "issues", "reviews", "comments", "releases"):
        assert "repo" in _columns(path, table)


def test_metadata_seeded(tmp_path):
    db = DatabaseManager(str(tmp_path / "fresh.db"))
    assert db.get_metadata("database_version") == "1.0"


def test_upsert_idempotent_by_repo_number(db_path):
    db = DatabaseManager(db_path)
    before = len(db.get_pull_requests(repo=MAIN))
    # Re-upsert an identical PR (same id and same repo/number) -> no new row.
    db.upsert_pull_request(_pr(1, MAIN, 1, "coredev", "2024-01-01T00:00:00",
                               "merged", "2024-01-03T00:00:00"))
    assert len(db.get_pull_requests(repo=MAIN)) == before


def test_upsert_replace_updates_fields(db_path):
    db = DatabaseManager(db_path)
    updated = _pr(1, MAIN, 1, "coredev", "2024-01-01T00:00:00",
                  "merged", "2024-01-03T00:00:00")
    updated["title"] = "brand new title"
    db.upsert_pull_request(updated)
    pr = db.get_pull_request_by_number(MAIN, 1)
    assert pr["title"] == "brand new title"


def test_upsert_replace_on_repo_number_collision(tmp_path):
    # A different id but the same (repo, number) must REPLACE, not duplicate,
    # because of the UNIQUE(repo, number) constraint.
    db = DatabaseManager(str(tmp_path / "c.db"))
    db.upsert_pull_request(_pr(1, MAIN, 1, "coredev", "2024-01-01T00:00:00", "open"))
    db.upsert_pull_request(_pr(999, MAIN, 1, "alice", "2024-01-01T00:00:00", "open"))
    rows = db.get_pull_requests(repo=MAIN)
    assert len(rows) == 1
    assert rows[0]["user_login"] == "alice"


def test_get_activity_timeline_no_indexerror(db_path):
    # Regression for bug 1: the SQL had two "{}" placeholders but was
    # .format(days)-ed with a single arg, raising IndexError on every call.
    db = DatabaseManager(db_path)
    assert isinstance(db.get_activity_timeline(), list)
    assert isinstance(db.get_activity_timeline(90), list)


def test_get_activity_timeline_counts_recent_rows(tmp_path):
    db = DatabaseManager(str(tmp_path / "recent.db"))
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    db.upsert_pull_request(_pr(1, MAIN, 1, "coredev", now, "open"))
    db.upsert_issue({
        "id": 1, "repo": MAIN, "number": 1, "title": "t", "state": "open",
        "created_at": now, "updated_at": now, "user_login": "alice",
    })
    rows = db.get_activity_timeline(3650)
    types = {r["type"] for r in rows}
    assert types == {"PR", "Issue"}


# --- migration -------------------------------------------------------------

# Pre-migration CREATE TABLE shapes (no `repo` column). These mimic the legacy
# schema that shipped before multi-repo support; the migration must rebuild them
# in place and stamp existing rows with the main repo.
_LEGACY_PR = """
    CREATE TABLE pull_requests (
        id INTEGER PRIMARY KEY,
        number INTEGER NOT NULL,
        title TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        user_login TEXT NOT NULL,
        base_branch TEXT NOT NULL,
        head_branch TEXT NOT NULL,
        last_fetched_at TEXT NOT NULL
    )
"""

_LEGACY_ISSUE = """
    CREATE TABLE issues (
        id INTEGER PRIMARY KEY,
        number INTEGER NOT NULL,
        title TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        user_login TEXT NOT NULL,
        last_fetched_at TEXT NOT NULL
    )
"""


def _build_legacy_db(path):
    conn = sqlite3.connect(str(path))
    conn.execute(_LEGACY_PR)
    conn.execute(_LEGACY_ISSUE)
    for n in (1, 2):
        conn.execute(
            "INSERT INTO pull_requests VALUES (?,?,?,?,?,?,?,?,?,?)",
            (n, n, f"PR {n}", "open", "2023-01-01T00:00:00", "2023-01-01T00:00:00",
             "someone", "main", "feat", "2023-01-01T00:00:00"),
        )
    for n in (1, 2):
        conn.execute(
            "INSERT INTO issues VALUES (?,?,?,?,?,?,?,?)",
            (n, n, f"Issue {n}", "open", "2023-01-01T00:00:00",
             "2023-01-01T00:00:00", "someone", "2023-01-01T00:00:00"),
        )
    conn.commit()
    conn.close()


def test_migration_adds_repo_column_and_preserves_rows(tmp_path):
    path = tmp_path / "legacy.db"
    _build_legacy_db(path)

    assert "repo" not in _columns(path, "pull_requests")

    # Opening with DatabaseManager triggers the auto-migration.
    db = DatabaseManager(str(path))

    assert "repo" in _columns(path, "pull_requests")
    assert "repo" in _columns(path, "issues")

    prs = db.get_pull_requests()
    issues = db.get_issues()
    assert len(prs) == 2
    assert len(issues) == 2
    # Existing rows are attributed to the main repo.
    assert {pr["repo"] for pr in prs} == {repos.MAIN_REPO}
    assert {issue["repo"] for issue in issues} == {repos.MAIN_REPO}


def test_migration_is_idempotent(tmp_path):
    path = tmp_path / "legacy.db"
    _build_legacy_db(path)
    DatabaseManager(str(path))
    # Second open must not re-migrate or corrupt anything.
    migrated = DatabaseManager(str(path))._migrate_to_repo_schema()
    assert migrated == []


def test_create_statements_match_repo_tables():
    # Guard: every repo-aware table declares a repo column in its CREATE stmt.
    stmts = DatabaseSchema.get_create_table_statements()
    for table in ("pull_requests", "issues", "reviews", "comments", "releases"):
        assert "repo TEXT NOT NULL" in stmts[table]
