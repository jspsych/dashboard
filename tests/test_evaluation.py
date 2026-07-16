"""
Sustainability-evaluation tests: rolling-window arithmetic, goal_series on the
fixture DB (exact values incl. goal4 union-not-sum and goal5 bucketing), and
current_vs_previous edge cases.
"""

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from src import evaluation


def _utc(y, m, d):
    return datetime(y, m, d, tzinfo=timezone.utc)


# --- window arithmetic -----------------------------------------------------

def test_quarter_start():
    assert evaluation._quarter_start(_utc(2024, 2, 17)) == _utc(2024, 1, 1)
    assert evaluation._quarter_start(_utc(2024, 5, 9)) == _utc(2024, 4, 1)
    assert evaluation._quarter_start(_utc(2024, 12, 31)) == _utc(2024, 10, 1)


def test_add_months_wraps_year():
    assert evaluation._add_months(_utc(2024, 1, 1), 3) == _utc(2024, 4, 1)
    assert evaluation._add_months(_utc(2024, 11, 1), 3) == _utc(2025, 2, 1)


def test_rolling_windows_quarter_aligned(db_path):
    windows = list(evaluation.rolling_windows(db_path))
    assert windows[:4] == [
        _utc(2024, 1, 1), _utc(2024, 4, 1), _utc(2024, 7, 1), _utc(2024, 10, 1)
    ]
    # every window end is quarter-aligned and not in the future.
    now = datetime.now(timezone.utc)
    for w in windows:
        assert w.month in (1, 4, 7, 10) and w.day == 1
        assert w <= now


def test_rolling_windows_step_months(db_path):
    windows = list(evaluation.rolling_windows(db_path, step_months=6))
    assert windows[:3] == [_utc(2024, 1, 1), _utc(2024, 7, 1), _utc(2025, 1, 1)]


def test_rolling_windows_empty_when_no_data(tmp_path):
    from src.database import DatabaseManager
    path = str(tmp_path / "empty.db")
    DatabaseManager(path)
    assert list(evaluation.rolling_windows(path)) == []


# --- goal_series -----------------------------------------------------------

def _row_for(df, window_end):
    return df[df["window_end"] == window_end].iloc[0]


def test_goal_series_keys(db_path):
    series = evaluation.goal_series(db_path)
    assert set(series) == {
        "goal1_core_team_size", "goal2_community_merge_time",
        "goal3_support_response", "goal4_community_contributions",
        "goal5_new_contributors",
    }


def test_goal1_core_team_size_constant(db_path):
    series = evaluation.goal_series(db_path)
    # frozen roster: 2 members with null dates -> size 2 at every window.
    assert set(series["goal1_core_team_size"]["value"]) == {2}


def test_goal4_union_not_sum(db_path):
    series = evaluation.goal_series(db_path)
    row = _row_for(series["goal4_community_contributions"], _utc(2024, 7, 1))
    # 2 community repos each with 1 PR + 1 issue -> 4 contributions.
    assert row["contributions"] == 4
    # contributors {alice (both repos), dave, erin} -> union of 3, not sum of 4.
    assert row["unique_contributors"] == 3


def test_goal5_bucketing(db_path):
    series = evaluation.goal_series(db_path)
    g5 = series["goal5_new_contributors"]
    assert _row_for(g5, _utc(2024, 1, 1))["new_contributors"] == 0
    assert _row_for(g5, _utc(2024, 4, 1))["new_contributors"] == 5
    assert _row_for(g5, _utc(2024, 7, 1))["new_contributors"] == 6


def test_goal5_commit_column(db_path):
    series = evaluation.goal_series(db_path)
    g5 = series["goal5_new_contributors"]
    assert "new_commit_contributors" in g5.columns
    # MAIN first non-merge commits: coredev (01-01), alice (01-05) -> both
    # bucket into the windows ending 2024-04-01 and 2024-07-01; the 2024-01-01
    # window excludes them (its exclusive end is 2024-01-01).
    assert _row_for(g5, _utc(2024, 1, 1))["new_commit_contributors"] == 0
    assert _row_for(g5, _utc(2024, 4, 1))["new_commit_contributors"] == 2
    assert _row_for(g5, _utc(2024, 7, 1))["new_commit_contributors"] == 2


def test_definitions_cover_every_goal():
    # Every goal with a statement must have a public methodology definition.
    assert set(evaluation.DEFINITIONS) == set(evaluation.GOAL_STATEMENTS)
    for key, text in evaluation.DEFINITIONS.items():
        assert isinstance(text, str) and text.strip()


def test_goal_columns_present(db_path):
    series = evaluation.goal_series(db_path)
    assert list(series["goal2_community_merge_time"].columns) == [
        "window_end", "community_median_days", "core_median_days",
        "community_merge_rate",
    ]
    assert list(series["goal3_support_response"].columns) == [
        "window_end", "median_first_response_days", "qa_answer_rate",
        "core_response_share_pct",
    ]


# --- current_vs_previous ---------------------------------------------------

def test_current_vs_previous_normal():
    df = pd.DataFrame({"v": [10.0, 11.0, 12.0]})
    result = evaluation.current_vs_previous(df, "v")
    # compares last (12) to two rows back (10): +20%.
    assert result == {"current": 12.0, "previous": 10.0, "change_pct": 20.0}


def test_current_vs_previous_nan_current():
    df = pd.DataFrame({"v": [10.0, 11.0, np.nan]})
    result = evaluation.current_vs_previous(df, "v")
    assert result["current"] is None
    assert result["change_pct"] is None


def test_current_vs_previous_zero_previous():
    df = pd.DataFrame({"v": [0.0, 5.0, 8.0]})
    result = evaluation.current_vs_previous(df, "v")
    # previous is 0 -> division guard yields None.
    assert result["previous"] == 0.0
    assert result["change_pct"] is None


def test_current_vs_previous_too_few_rows():
    df = pd.DataFrame({"v": [10.0, 11.0]})
    result = evaluation.current_vs_previous(df, "v")
    assert result["previous"] is None
    assert result["change_pct"] is None
