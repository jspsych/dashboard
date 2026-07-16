"""
PR metric tests: merge time, merge rate, first-response (bugs 2 & 3),
core/community group splits with exact medians, and repo isolation.
"""

import pytest
from conftest import CONTRIB, MAIN


@pytest.fixture
def calc(make_calc):
    # All-time, main repo, no upper bound -> deterministic on fixed 2024 data.
    return make_calc(days=None, repo=MAIN)


def test_median_pr_merge_time(calc):
    # merged main-repo merge times (days): [2, 4, 6, 16, 1] -> median 4.
    assert calc.get_median_pr_merge_time() == 4.0


def test_median_pr_merge_time_by_group(calc):
    # core: [2, 4] -> 3 ; community: [6, 16, 1] -> 6.
    assert calc.get_median_pr_merge_time_by_group() == {"core": 3.0, "community": 6.0}


def test_pr_merge_rate(calc):
    # 5 merged of 6 non-bot PRs (the 7th is a bot and excluded).
    result = calc.get_pr_merge_rate()
    assert result["total_merged"] == 5
    assert result["total_prs"] == 6
    assert result["merge_rate"] == pytest.approx(100 * 5 / 6)


def test_pr_merge_rate_by_group(calc):
    result = calc.get_pr_merge_rate_by_group()
    assert result["core"] == {"merge_rate": 100.0, "total_merged": 2, "total_prs": 2}
    assert result["community"]["total_prs"] == 4
    assert result["community"]["total_merged"] == 3
    assert result["community"]["merge_rate"] == pytest.approx(75.0)


def test_bot_prs_excluded_from_merge_time(calc):
    # The bot PR (#6) merges in 1 day; if it were counted the overall median
    # would shift. That it doesn't confirms the central bot filter (bug 3).
    assert calc.get_median_pr_merge_time() == 4.0
    assert calc.get_pr_merge_rate()["total_prs"] == 6  # not 7


def test_pr_first_response(calc):
    # non-bot first responses (days): PR1=1, PR2=3, PR3=5, PR4=7 -> median 4.
    assert calc.get_med_time_to_first_response_prs() == 4.0


def test_pr_first_response_excludes_bots(calc):
    # PR1's earliest comment is a dependabot[bot] comment at 0.25d; the human
    # response at 1d is what counts. If the bot counted the median would drop.
    assert calc.get_med_time_to_first_response_prs() == 4.0


def test_pr_vs_issue_first_response_differ(calc):
    # Bug 2: the PR page must show the PR metric, not the issue metric.
    pr = calc.get_med_time_to_first_response_prs()
    issue = calc.get_time_to_first_response_issue()
    assert pr == 4.0
    assert issue == 3.25
    assert pr != issue


def test_pr_first_response_by_group(calc):
    # core author PRs: [1, 3] -> 2 ; community author PRs: [5, 7] -> 6.
    assert calc.get_med_time_to_first_response_prs_by_group() == {
        "core": 2.0, "community": 6.0
    }


def test_repo_isolation_community_not_in_main(calc):
    # Community-repo PRs (contrib #100 merges in 1 day) must never leak into
    # main-repo metrics.
    assert calc.get_median_pr_merge_time_by_group()["community"] == 6.0
    assert calc.get_pr_merge_rate()["total_prs"] == 6


def test_community_repo_metrics_isolated(make_calc):
    contrib = make_calc(days=None, repo=CONTRIB)
    assert contrib.get_median_pr_merge_time() == 1.0
    assert contrib.get_pr_merge_rate()["total_prs"] == 1


def test_all_repos_combined(make_calc):
    # repo=None includes every repo: 6 non-bot main PRs + 2 community PRs.
    everything = make_calc(days=None, repo=None)
    assert everything.get_pr_merge_rate()["total_prs"] == 8


def test_windowed_end_date_excludes_later_prs(make_calc):
    # An explicit end_date caps the view: PR #7 (April) is excluded before it.
    calc = make_calc(days=None, repo=MAIN, end_date="2024-02-01T00:00:00")
    # Only Jan merged PRs remain: [2, 4, 6, 16] -> median 5.
    assert calc.get_median_pr_merge_time() == 5.0
    assert calc.get_median_pr_merge_time_by_group()["community"] == 11.0
