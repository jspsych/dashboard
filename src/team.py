"""
Core team roster utilities.

Loads the maintained core-team roster from config/team.json and provides
helpers for computing the Goal 1 sustainability metric (number of contributors
with merge access) and for filtering out bot accounts from metrics.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Set

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "team.json"


def load_team(config_path: Optional[str] = None) -> dict:
    """
    Load the core-team roster configuration from config/team.json.

    The default path is resolved relative to this module file, so it works
    regardless of the current working directory (repo root or dashboard/
    render context).

    Args:
        config_path (Optional[str]): Optional override path to the config file.

    Returns:
        dict: The parsed team configuration.
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO YYYY-MM-DD date string into a timezone-aware datetime."""
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def core_team_logins(at_date: Optional[datetime] = None) -> Set[str]:
    """
    Return the set of logins with merge access at the given date.

    An entry counts if merge_access_since is null or <= at_date, and
    merge_access_until is null or > at_date.

    Args:
        at_date (Optional[datetime]): The date to evaluate membership at.
                                      Defaults to now (UTC).

    Returns:
        Set[str]: The logins with merge access at at_date.
    """
    if at_date is None:
        at_date = datetime.now(timezone.utc)
    elif at_date.tzinfo is None:
        at_date = at_date.replace(tzinfo=timezone.utc)

    team = load_team()
    logins = set()
    for entry in team.get("core_team", []):
        since = _parse_date(entry.get("merge_access_since"))
        until = _parse_date(entry.get("merge_access_until"))
        if since is not None and since > at_date:
            continue
        if until is not None and until <= at_date:
            continue
        logins.add(entry["login"])
    return logins


def core_team_size(at_date: Optional[datetime] = None) -> int:
    """
    Return the number of contributors with merge access at the given date.

    This is the Goal 1 sustainability metric.

    Args:
        at_date (Optional[datetime]): The date to evaluate at. Defaults to now.

    Returns:
        int: The size of the core team at at_date.
    """
    return len(core_team_logins(at_date))


def bot_logins() -> Set[str]:
    """
    Return the set of known bot logins from the config's "bots" key.

    Returns:
        Set[str]: The configured bot logins.
    """
    return set(load_team().get("bots", []))


def is_bot(login: Optional[str], user_type: Optional[str] = None) -> bool:
    """
    Determine whether a given account is a bot.

    Args:
        login (Optional[str]): The account login.
        user_type (Optional[str]): The GitHub user type, e.g. 'User' or 'Bot'.

    Returns:
        bool: True if user_type == 'Bot', the login is a known bot, or the
              login ends with '[bot]'.
    """
    if user_type == "Bot":
        return True
    if login is None:
        return False
    if login in bot_logins():
        return True
    return login.endswith("[bot]")
