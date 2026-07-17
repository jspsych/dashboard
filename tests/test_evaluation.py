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
        "community_merge_rate", "community_n", "core_n",
    ]
    assert list(series["goal3_support_response"].columns) == [
        "window_end", "median_first_response_days", "qa_answer_rate",
        "core_response_share_pct", "n_discussions",
    ]


def test_goal2_sample_size_columns(db_path):
    # Window ending 2024-07-01 spans all of 2024-H1 (182-day trailing window).
    # Merged community PRs in it: #3, #4, #7 -> 3; merged core PRs: #1, #2 -> 2.
    series = evaluation.goal_series(db_path)
    row = _row_for(series["goal2_community_merge_time"], _utc(2024, 7, 1))
    assert row["community_n"] == 3
    assert row["core_n"] == 2
    # Sanity: the medians still line up with the group merge-time metric.
    assert row["community_median_days"] == 6.0
    assert row["core_median_days"] == 3.0


def test_goal3_n_discussions(db_path):
    # All three fixture discussions are created 2024-01-01, so they fall inside
    # the window ending 2024-07-01.
    series = evaluation.goal_series(db_path)
    row = _row_for(series["goal3_support_response"], _utc(2024, 7, 1))
    assert row["n_discussions"] == 3


def test_goal3_starts_at_discussions_launch(db_path, monkeypatch):
    # The support series must not report windows before Discussions existed.
    # Patch the launch date forward into the fixture's own history and confirm
    # the earlier windows are dropped (while the other goals are untouched).
    monkeypatch.setattr(evaluation, "DISCUSSIONS_LAUNCH", "2024-07-01")
    series = evaluation.goal_series(db_path)
    g3 = series["goal3_support_response"]
    launch = pd.Timestamp("2024-07-01", tz="UTC")
    assert (pd.to_datetime(g3["window_end"], utc=True) >= launch).all()
    assert _utc(2024, 1, 1) not in set(g3["window_end"])
    # Other goals keep their full window history.
    assert _utc(2024, 1, 1) in set(series["goal1_core_team_size"]["window_end"])


def test_default_discussions_launch_keeps_all_2024_windows(db_path):
    # With the real launch (2020-05), no 2024 fixture window is dropped.
    series = evaluation.goal_series(db_path)
    g3 = series["goal3_support_response"]
    assert _utc(2024, 1, 1) in set(g3["window_end"])


# --- GOAL_DIRECTION / describe_change --------------------------------------

def test_goal_direction_covers_every_goal():
    assert set(evaluation.GOAL_DIRECTION) == set(evaluation.GOAL_STATEMENTS)
    assert set(evaluation.GOAL_METRIC_KIND) == set(evaluation.GOAL_STATEMENTS)
    for value in evaluation.GOAL_DIRECTION.values():
        assert value in ("lower", "higher")
    for value in evaluation.GOAL_METRIC_KIND.values():
        assert value in ("time", "count")


def test_describe_change_time_improved():
    # Merge time down 86% is faster and, since lower is better, an improvement.
    assert evaluation.describe_change(-86.0, "lower", "time") == \
        "▼ 86.0% faster — improved"


def test_describe_change_time_worsened():
    # Response time up 141.9% is slower and, since lower is better, worse.
    assert evaluation.describe_change(141.9, "lower", "time") == \
        "▲ 141.9% slower — worsened"


def test_describe_change_count_improved():
    # Contributions up 12% is more and, since higher is better, an improvement.
    assert evaluation.describe_change(12.0, "higher", "count") == \
        "▲ 12.0% more — improved"


def test_describe_change_count_worsened():
    # Contributor count down 20% is fewer and, since higher is better, worse.
    assert evaluation.describe_change(-20.0, "higher", "count") == \
        "▼ 20.0% fewer — worsened"


def test_describe_change_no_change():
    assert evaluation.describe_change(0.0, "lower", "time") == "no change"
    # Rounds to 0.0% -> flat, not a spurious "0.0% faster".
    assert evaluation.describe_change(0.03, "higher", "count") == "no change"


def test_describe_change_missing():
    assert evaluation.describe_change(None, "lower", "time") == "–"
    assert evaluation.describe_change(float("nan"), "higher", "count") == "–"


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
