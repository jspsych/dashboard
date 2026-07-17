"""
Issue metric tests: close time, close rate, and issue first-response.
"""

import pytest
from conftest import MAIN


@pytest.fixture
def calc(make_calc):
    return make_calc(days=None, repo=MAIN)


def test_median_issue_close_time(calc):
    # closed main-repo issues (days): [2, 10] -> median 6.
    assert calc.get_median_issue_close_time() == 6.0


def test_issue_close_rate(calc):
    result = calc.get_issue_close_rate()
    assert result["total_closed"] == 2
    assert result["total_issues"] == 4
    assert result["close_rate"] == pytest.approx(50.0)


def test_issue_first_response(calc):
    # issue first responses (days): issue1=1.5, issue2=5 -> median 3.25.
    assert calc.get_time_to_first_response_issue() == 3.25


def test_issue_first_response_excludes_bots(calc):
    # issue2's earliest comment is a changeset-bot comment; the human response
    # at 5d is what counts. The result stays 3.25 (bug 3 filter).
    assert calc.get_time_to_first_response_issue() == 3.25


def test_windowed_end_date_excludes_later_issues(make_calc):
    # end_date before April drops issue #4; close metrics are unchanged since
    # both closed issues are in January.
    calc = make_calc(days=None, repo=MAIN, end_date="2024-02-01T00:00:00")
    result = calc.get_issue_close_rate()
    assert result["total_issues"] == 3  # #1, #2, #3 (not #4)
    assert result["total_closed"] == 2
