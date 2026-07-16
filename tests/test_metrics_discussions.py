"""
Discussion / support metric tests (Goal 3): first non-author response (with
self-responses and bots excluded), time-to-answer, answer rate including the
is_answered NULL handling (bug 4), qa vs overall, response share, and responder
diversity.
"""

import pytest
from conftest import MAIN


@pytest.fixture
def calc(make_calc):
    return make_calc(days=None, repo=MAIN)


def test_first_discussion_response(calc):
    # per-discussion first non-author, non-bot response (days):
    #   disc1 = 1 (bot at 0.5d excluded), disc2 = 2 (self reply excluded),
    #   disc3 = 5  -> median 2.
    assert calc.get_med_time_to_first_discussion_response() == 2.0


def test_first_response_excludes_self_and_bots(calc):
    # If the self reply (disc2 @1d) or the bot reply (disc1 @0.5d) counted, the
    # median would move off 2.0.
    assert calc.get_med_time_to_first_discussion_response() == 2.0


def test_time_to_answer(calc):
    # only disc1 is answered; answer posted 3 days after creation.
    assert calc.get_med_time_to_answer() == 3.0


def test_answer_rate_null_not_counted(calc):
    # Bug 4: disc3 (General) has is_answered NULL and must NOT count as
    # answered. 1 of 3 answered -> 33.3%, not 2 of 3.
    result = calc.get_discussion_answer_rate()
    assert result["total_discussions"] == 3
    assert result["total_answered"] == 1
    assert result["answer_rate"] == pytest.approx(100 / 3)


def test_qa_answer_rate_vs_overall(calc):
    # Q&A: disc1, disc2 -> 1 of 2 answered = 50%, distinct from the 33.3%
    # overall rate.
    result = calc.get_discussion_answer_rate()
    assert result["qa_total"] == 2
    assert result["qa_answered"] == 1
    assert result["qa_answer_rate"] == pytest.approx(50.0)
    assert result["qa_answer_rate"] != result["answer_rate"]


def test_response_share_core_vs_community(calc):
    # discussion comments (bots excluded): coredev x3 (core), alice + bob
    # (community) -> 5 total, 60% core / 40% community.
    result = calc.get_response_share()
    assert result["total_responses"] == 5
    assert result["core_team_pct"] == pytest.approx(60.0)
    assert result["community_pct"] == pytest.approx(40.0)


def test_responder_diversity(calc):
    df = calc.get_responder_diversity()
    assert len(df) == 1  # all comments fall in one quarter
    row = df.iloc[0]
    assert row["unique_responders"] == 3          # coredev, alice, bob
    assert row["core_team_responders"] == 1       # coredev
    assert row["community_responders"] == 2       # alice, bob
