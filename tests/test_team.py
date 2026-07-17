"""
Team roster tests: core_team_logins date-window boundaries and is_bot logic.
Each test installs its own fake roster so it never depends on config/team.json.
"""

from datetime import datetime, timezone

from src import team

_ROSTER = {
    "core_team": [
        {"login": "always", "merge_access_since": None, "merge_access_until": None},
        {"login": "joined", "merge_access_since": "2024-06-01", "merge_access_until": None},
        {"login": "left", "merge_access_since": None, "merge_access_until": "2024-06-01"},
        {"login": "window", "merge_access_since": "2024-01-01",
         "merge_access_until": "2024-12-31"},
    ],
    "bots": ["changeset-bot", "renovate"],
}


def _install(monkeypatch, roster=_ROSTER):
    monkeypatch.setattr(team, "load_team", lambda config_path=None: roster)


def _utc(y, m, d):
    return datetime(y, m, d, tzinfo=timezone.utc)


def test_core_team_since_boundary_inclusive(monkeypatch):
    _install(monkeypatch)
    # merge_access_since == at_date counts as a member (since <= at_date).
    assert "joined" in team.core_team_logins(_utc(2024, 6, 1))
    assert "joined" not in team.core_team_logins(_utc(2024, 5, 31))


def test_core_team_until_boundary_exclusive(monkeypatch):
    _install(monkeypatch)
    # merge_access_until == at_date means access has ended (until <= at_date).
    assert "left" not in team.core_team_logins(_utc(2024, 6, 1))
    assert "left" in team.core_team_logins(_utc(2024, 5, 31))


def test_core_team_membership_sets(monkeypatch):
    _install(monkeypatch)
    assert team.core_team_logins(_utc(2024, 6, 1)) == {"always", "joined", "window"}
    assert team.core_team_logins(_utc(2024, 5, 31)) == {"always", "left", "window"}


def test_core_team_window_excluded_outside(monkeypatch):
    _install(monkeypatch)
    assert "window" not in team.core_team_logins(_utc(2023, 12, 31))
    assert "window" not in team.core_team_logins(_utc(2024, 12, 31))  # until exclusive


def test_core_team_size(monkeypatch):
    _install(monkeypatch)
    assert team.core_team_size(_utc(2024, 6, 1)) == 3


def test_is_bot_user_type(monkeypatch):
    _install(monkeypatch)
    assert team.is_bot("anyone", "Bot") is True


def test_is_bot_known_login(monkeypatch):
    _install(monkeypatch)
    assert team.is_bot("changeset-bot") is True
    assert team.is_bot("renovate") is True


def test_is_bot_suffix(monkeypatch):
    _install(monkeypatch)
    assert team.is_bot("github-actions[bot]") is True


def test_is_bot_human(monkeypatch):
    _install(monkeypatch)
    assert team.is_bot("alice", "User") is False


def test_is_bot_none_login(monkeypatch):
    _install(monkeypatch)
    assert team.is_bot(None) is False
    assert team.is_bot(None, "Bot") is True


def test_bot_logins(monkeypatch):
    _install(monkeypatch)
    assert team.bot_logins() == {"changeset-bot", "renovate"}
