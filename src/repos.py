"""
Tracked-repository configuration utilities.

Loads the list of repositories tracked by the analytics pipeline from
config/repos.json and provides helpers for enumerating them, optionally
filtered by role ('core' or 'community').
"""

import json
from pathlib import Path
from typing import List, Optional

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "repos.json"

# The primary/core library repo. Existing data (pre multi-repo support) belongs
# to this repo, and metrics default to it so historical dashboard pages are
# unaffected by community-repo data.
MAIN_REPO = "jspsych/jsPsych"


def load_repos(config_path: Optional[str] = None) -> dict:
    """
    Load the tracked-repository configuration from config/repos.json.

    The default path is resolved relative to this module file, so it works
    regardless of the current working directory (repo root or dashboard/
    render context).

    Args:
        config_path (Optional[str]): Optional override path to the config file.

    Returns:
        dict: The parsed repos configuration.
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def repo_names(role: Optional[str] = None) -> List[str]:
    """
    Return the list of tracked repository full names ("owner/name").

    Args:
        role (Optional[str]): If provided, only repos with this role
                              ('core' or 'community') are returned.

    Returns:
        List[str]: The full names of the matching repositories.
    """
    repos = load_repos().get("repos", [])
    if role is not None:
        repos = [r for r in repos if r.get("role") == role]
    return [r["full_name"] for r in repos]
