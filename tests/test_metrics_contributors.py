"""
Contributor metric tests (Goals 4 & 5): contribution rate, new-contributor
timeline (first-ever-across-history semantics), cumulative contributors, and the
contributor summary.
"""

import pandas as pd
import pytest
from conftest import MAIN


@pytest.fixture
def calc(make_calc):
    return make_calc(days=None, repo=MAIN)


def _by_quarter(df, period_col="period"):
    return {row[period_col].date().isoformat(): row for _, row in df.iterrows()}


def test_contribution_rate(calc):
    rows = _by_quarter(calc.get_contribution_rate())
    q1 = rows["2024-03-31"]
    q2 = rows["2024-06-30"]
    assert (q1["merged_prs"], q1["opened_issues"]) == (4, 3)
    assert (q2["merged_prs"], q2["opened_issues"]) == (1, 1)


def test_new_contributors_timeline(calc):
    rows = _by_quarter(calc.get_new_contributors_timeline())
    # first-ever contributions: coredev, jodeleeuw, alice, bob, newbie in Q1;
    # carol in Q2.
    assert rows["2024-03-31"]["new_contributors"] == 5
    assert rows["2024-06-30"]["new_contributors"] == 1


def test_cumulative_contributors(calc):
    df = calc.get_cumulative_contributors()
    # cumulative runs 5 -> 6; final equals the repo's distinct contributor count.
    assert list(df["cumulative_contributors"]) == [5, 6]


def test_contributor_summary_all_time(calc):
    summary = calc.get_contributor_summary()
    assert summary["total_contributors"] == 6
    # all-time window -> every contributor is "new".
    assert summary["new_contributors"] == 6
    assert summary["total_contributions"] == 16


def test_new_contributor_semantics_active_but_not_new(make_calc):
    # Window covering April 2024. alice is active (opens issue #4) but her
    # first-ever contribution was in January, so she is NOT new; carol's first
    # contribution is in this window, so she IS new.
    calc = make_calc(days=31, repo=MAIN, end_date="2024-04-15T00:00:00")
    summary = calc.get_contributor_summary()
    assert summary["total_contributors"] == 2   # alice, carol
    assert summary["new_contributors"] == 1      # carol only
    assert summary["total_contributions"] == 2   # PR #7 + issue #4


def test_first_contribution_dates_span_full_history(make_calc):
    # Even a narrow window sees each contributor's first-EVER contribution,
    # so first-contribution dates don't drift with the window.
    calc = make_calc(days=31, repo=MAIN, end_date="2024-04-15T00:00:00")
    first = calc._first_contribution_dates()
    assert first["carol"] == pd.Timestamp("2024-04-01T00:00:00", tz="UTC")
    assert first["alice"] == pd.Timestamp("2024-01-01T00:00:00", tz="UTC")
