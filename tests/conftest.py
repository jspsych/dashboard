"""
Shared pytest fixtures for the dashboard analytics test suite.

Everything here is deterministic and self-contained: a small synthetic SQLite
database built in ``tmp_path`` with hand-computable expected values, plus a
frozen fake team roster / repo config so tests never depend on the real
``data/analytics.db``, ``config/team.json``, ``config/repos.json``, the network,
or today's date.
"""

import pytest

from src import repos, team
from src.database import DatabaseManager
from src.metrics_calculator import MetricsCalculator

# Repository full names used throughout the fixtures.
MAIN = "jspsych/jsPsych"
CONTRIB = "jspsych/jspsych-contrib"
TIMELINES = "jspsych/jspsych-timelines"

# Frozen fake roster. All core members have null merge-access dates, so
# core_team_logins() == {"coredev", "jodeleeuw"} at any date and
# core_team_size() == 2 constant. This is intentionally decoupled from the
# real config/team.json so tests don't break when the roster is edited.
FAKE_TEAM = {
    "core_team": [
        {"login": "coredev", "merge_access_since": None, "merge_access_until": None},
        {"login": "jodeleeuw", "merge_access_since": None, "merge_access_until": None},
    ],
    "bots": ["changeset-bot"],
}

FAKE_REPOS = {
    "repos": [
        {"full_name": MAIN, "role": "core"},
        {"full_name": CONTRIB, "role": "community"},
        {"full_name": TIMELINES, "role": "community"},
    ]
}


@pytest.fixture(autouse=True)
def frozen_config(monkeypatch):
    """Freeze the team roster and repo config for every test.

    Patches the loaders (not the individual helpers) so all downstream logic
    -- is_bot, core_team_logins, repo_names -- runs against the fixed fake
    config regardless of what config/team.json currently contains.
    """
    monkeypatch.setattr(team, "load_team", lambda config_path=None: FAKE_TEAM)
    monkeypatch.setattr(repos, "load_repos", lambda config_path=None: FAKE_REPOS)


def _pr(id, repo, number, user_login, created_at, state,
        merged_at=None, user_type="User", updated_at=None):
    return {
        "id": id, "repo": repo, "number": number,
        "title": f"PR {number}", "body": "", "state": state,
        "created_at": created_at, "updated_at": updated_at or merged_at or created_at,
        "closed_at": merged_at, "merged_at": merged_at,
        "user_login": user_login, "user_type": user_type,
        "base_branch": "main", "head_branch": "feature",
        "labels": [], "assignees": [],
    }


def _issue(id, repo, number, user_login, created_at, state,
           closed_at=None, user_type="User"):
    return {
        "id": id, "repo": repo, "number": number,
        "title": f"Issue {number}", "body": "", "state": state,
        "created_at": created_at, "updated_at": closed_at or created_at,
        "closed_at": closed_at, "user_login": user_login, "user_type": user_type,
        "labels": [], "comments_count": 0,
    }


def _review(id, repo, pr_number, reviewer_login, submitted_at, state="COMMENTED"):
    return {
        "id": id, "repo": repo, "pr_number": pr_number,
        "reviewer_login": reviewer_login, "state": state,
        "submitted_at": submitted_at, "body": "",
    }


def _comment(id, repo, user_login, created_at, comment_type,
             issue_number=None, pr_number=None):
    return {
        "id": id, "repo": repo, "issue_number": issue_number, "pr_number": pr_number,
        "user_login": user_login, "body": "x", "created_at": created_at,
        "updated_at": created_at, "comment_type": comment_type,
    }


def _discussion(id, repo, number, author_login, created_at, category,
                is_answered, answer_created_at=None, answer_comment_id=None):
    return {
        "id": id, "repo": repo, "number": number, "title": f"Disc {number}",
        "body": "", "category": category, "created_at": created_at,
        "updated_at": created_at, "author_login": author_login,
        "is_answered": is_answered, "answer_comment_id": answer_comment_id,
        "answer_created_at": answer_created_at, "upvote_count": 0,
        "comments_count": 0,
    }


def _dcomment(id, repo, discussion_number, author_login, created_at,
              is_answer=False, parent_comment_id=None):
    return {
        "id": id, "repo": repo, "discussion_number": discussion_number,
        "author_login": author_login, "body": "x", "created_at": created_at,
        "is_answer": is_answer, "parent_comment_id": parent_comment_id,
    }


def _commit(repo, sha, author_login, authored_at, author_name=None,
            author_email=None, committed_at=None, message_headline=None,
            is_merge_commit=False):
    return {
        "repo": repo, "sha": sha, "author_login": author_login,
        "author_name": author_name or (author_login or "Unknown"),
        "author_email": author_email or (
            f"{author_login}@example.com" if author_login else "external@example.com"),
        "authored_at": authored_at,
        "committed_at": committed_at or authored_at,
        "message_headline": message_headline or f"commit {sha}",
        "is_merge_commit": is_merge_commit,
    }


def build_fixture_db(path):
    """Build the synthetic analytics DB used by the metric/evaluation tests.

    All timestamps are fixed in 2024 so windowed metrics never drift with the
    clock. See the module-level design notes in the test files for the exact
    hand-computed expectations. Returns the DatabaseManager.
    """
    db = DatabaseManager(str(path))

    # --- Pull requests (main repo) ---
    # merge times (days): #1=2 core, #2=4 core, #3=6 comm, #4=16 comm, #7=1 comm.
    # #6 is a bot (user_type Bot) that MUST be excluded. #5 is open.
    db.upsert_pull_request(_pr(1, MAIN, 1, "coredev", "2024-01-01T00:00:00",
                               "merged", "2024-01-03T00:00:00"))
    db.upsert_pull_request(_pr(2, MAIN, 2, "jodeleeuw", "2024-01-01T00:00:00",
                               "merged", "2024-01-05T00:00:00"))
    db.upsert_pull_request(_pr(3, MAIN, 3, "alice", "2024-01-01T00:00:00",
                               "merged", "2024-01-07T00:00:00"))
    db.upsert_pull_request(_pr(4, MAIN, 4, "bob", "2024-01-01T00:00:00",
                               "merged", "2024-01-17T00:00:00"))
    db.upsert_pull_request(_pr(5, MAIN, 5, "alice", "2024-01-10T00:00:00", "open"))
    db.upsert_pull_request(_pr(6, MAIN, 6, "ci-bot", "2024-01-01T00:00:00",
                               "merged", "2024-01-02T00:00:00", user_type="Bot"))
    db.upsert_pull_request(_pr(7, MAIN, 7, "carol", "2024-04-01T00:00:00",
                               "merged", "2024-04-02T00:00:00"))

    # --- Issues (main repo) ---
    # close times (days): #1=2, #2=10. #3, #4 open.
    db.upsert_issue(_issue(1, MAIN, 1, "alice", "2024-01-01T00:00:00",
                           "closed", "2024-01-03T00:00:00"))
    db.upsert_issue(_issue(2, MAIN, 2, "bob", "2024-01-01T00:00:00",
                           "closed", "2024-01-11T00:00:00"))
    db.upsert_issue(_issue(3, MAIN, 3, "newbie", "2024-01-05T00:00:00", "open"))
    db.upsert_issue(_issue(4, MAIN, 4, "alice", "2024-04-01T00:00:00", "open"))

    # --- Reviews (main repo) ---
    db.upsert_review(_review(1, MAIN, 2, "jodeleeuw", "2024-01-04T00:00:00"))
    db.upsert_review(_review(2, MAIN, 4, "coredev", "2024-01-08T00:00:00"))

    # --- Comments (main repo) ---
    # PR first-response uses first non-bot comment/review. Bots come FIRST and
    # must be excluded (bug 3).
    db.upsert_comment(_comment(1, MAIN, "dependabot[bot]", "2024-01-01T06:00:00",
                               "pr", pr_number=1))          # bot, excluded
    db.upsert_comment(_comment(2, MAIN, "coredev", "2024-01-02T00:00:00",
                               "pr", pr_number=1))          # first response PR1 = 1d
    db.upsert_comment(_comment(3, MAIN, "coredev", "2024-01-06T00:00:00",
                               "pr", pr_number=3))          # first response PR3 = 5d
    db.upsert_comment(_comment(10, MAIN, "coredev", "2024-01-02T12:00:00",
                               "issue", issue_number=1))    # issue1 first resp = 1.5d
    db.upsert_comment(_comment(12, MAIN, "changeset-bot", "2024-01-02T00:00:00",
                               "issue", issue_number=2))    # bot, excluded
    db.upsert_comment(_comment(11, MAIN, "coredev", "2024-01-06T00:00:00",
                               "issue", issue_number=2))    # issue2 first resp = 5d

    # --- Discussions (main repo) ---
    db.upsert_discussion(_discussion(1, MAIN, 1, "alice", "2024-01-01T00:00:00",
                                     "Q&A", True, "2024-01-04T00:00:00", 2))
    db.upsert_discussion(_discussion(2, MAIN, 2, "bob", "2024-01-01T00:00:00",
                                     "Q&A", False))
    db.upsert_discussion(_discussion(3, MAIN, 3, "newbie", "2024-01-01T00:00:00",
                                     "General", None))  # is_answered NULL (bug 4)

    # --- Discussion comments (main repo) ---
    db.upsert_discussion_comment(_dcomment(1, MAIN, 1, "coredev", "2024-01-02T00:00:00"))
    db.upsert_discussion_comment(_dcomment(2, MAIN, 1, "coredev", "2024-01-04T00:00:00",
                                           is_answer=True, parent_comment_id=1))
    db.upsert_discussion_comment(_dcomment(3, MAIN, 2, "alice", "2024-01-03T00:00:00"))
    db.upsert_discussion_comment(_dcomment(4, MAIN, 3, "coredev", "2024-01-06T00:00:00"))
    # self-response by the discussion author -- must NOT count as first response.
    db.upsert_discussion_comment(_dcomment(5, MAIN, 2, "bob", "2024-01-02T00:00:00"))
    # bot response first on disc 1 -- must be excluded (bug 3).
    db.upsert_discussion_comment(_dcomment(6, MAIN, 1, "github-actions[bot]",
                                           "2024-01-01T12:00:00"))

    # --- Community repos (Goal 4). alice contributes to BOTH -> union not sum. ---
    db.upsert_pull_request(_pr(100, CONTRIB, 100, "alice", "2024-05-01T00:00:00",
                               "merged", "2024-05-02T00:00:00"))
    db.upsert_issue(_issue(100, CONTRIB, 100, "dave", "2024-05-01T00:00:00", "open"))
    db.upsert_pull_request(_pr(200, TIMELINES, 200, "alice", "2024-05-03T00:00:00",
                               "merged", "2024-05-04T00:00:00"))
    db.upsert_issue(_issue(200, TIMELINES, 200, "erin", "2024-05-03T00:00:00", "open"))

    # --- Commits (Goal 5). Two repos; one NULL-login commit (email not linked
    # to an account) and one merge commit that Goal-5 metrics should exclude. ---
    # MAIN: coredev + alice non-merge, one null-login non-merge, one merge.
    db.upsert_commit(_commit(MAIN, "sha_main_1", "coredev", "2024-01-01T00:00:00"))
    db.upsert_commit(_commit(MAIN, "sha_main_2", "alice", "2024-01-05T00:00:00"))
    db.upsert_commit(_commit(MAIN, "sha_main_3", None, "2024-01-10T00:00:00",
                             author_name="External Dev"))
    db.upsert_commit(_commit(MAIN, "sha_main_4", "coredev", "2024-01-15T00:00:00",
                             is_merge_commit=True))
    # CONTRIB: alice (also commits to MAIN -> union not sum) + dave.
    db.upsert_commit(_commit(CONTRIB, "sha_contrib_1", "alice", "2024-05-01T00:00:00"))
    db.upsert_commit(_commit(CONTRIB, "sha_contrib_2", "dave", "2024-05-02T00:00:00"))

    return db


@pytest.fixture
def db_path(tmp_path):
    """Path to a freshly built synthetic analytics DB."""
    path = tmp_path / "analytics.db"
    build_fixture_db(path)
    return str(path)


@pytest.fixture
def make_calc(db_path):
    """Factory for a MetricsCalculator against the fixture DB."""
    def _make(days=None, end_date=None, repo=MAIN):
        return MetricsCalculator(db_path=db_path, days=days, repo=repo,
                                 end_date=end_date)
    return _make
