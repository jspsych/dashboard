"""
Sustainability-evaluation engine.

Computes per-goal time series over ROLLING evaluation windows stepped across
the project's history, so change patterns (not just point-in-time values) are
visible. Windows are fixed-length look-backs (``window_days``, default 182 ~=
6 months) whose *end* is advanced ``step_months`` at a time (default quarterly)
across all of history. This is deliberately different from fixed
grant-anchored periods: the PI wants to see how each metric moves as the same
6-month lens slides forward.

The five sustainability goals:
  1. Increase the number of contributors with merge access.
  2. Reduce the time for code review and PR merges of community contributions.
  3. Decrease the time for support responses and broaden participation.
  4. Increase the rate of contribution to community repositories.
  5. Accelerate new-contributor growth on core packages.

Public API:
  - goal_series(...)           -> dict[str, pd.DataFrame], one series per goal.
  - current_vs_previous(...)   -> non-overlapping current-vs-6-months-ago delta.
"""

from datetime import datetime, timezone
from typing import Dict, Iterator, Optional, Set

import pandas as pd

from . import repos, team
from .metrics_calculator import MetricsCalculator

# Tables (and their creation-timestamp column) scanned to find the earliest
# relevant data point when deciding where the first rolling window starts.
_DATA_TABLES = {
    'pull_requests': 'created_at',
    'issues': 'created_at',
    'comments': 'created_at',
    'reviews': 'submitted_at',
    'discussions': 'created_at',
    'discussion_comments': 'created_at',
}

# One-line goal statements, surfaced in the CLI report.
GOAL_STATEMENTS = {
    'goal1_core_team_size':
        'Increase the number of contributors with merge access.',
    'goal2_community_merge_time':
        'Reduce the time for code review and PR merges of community contributions.',
    'goal3_support_response':
        'Decrease the time for support responses and broaden participation.',
    'goal4_community_contributions':
        'Increase the rate of contribution to community repositories.',
    'goal5_new_contributors':
        'Accelerate new-contributor growth on core packages.',
}

# The headline column used for the current-vs-previous comparison per goal.
GOAL_HEADLINE_COLUMN = {
    'goal1_core_team_size': 'value',
    'goal2_community_merge_time': 'community_median_days',
    'goal3_support_response': 'median_first_response_days',
    'goal4_community_contributions': 'contributions',
    'goal5_new_contributors': 'new_contributors',
}

# Plain-language methodology statements for each goal's headline metric. These
# are PUBLIC definitions -- the governance board and the public read them to
# know exactly how each number is computed. They spell out the evaluation
# window, what counts, and what is excluded (bots, merge commits,
# self-responses, etc.). Keep them precise; changing a definition changes what
# the reported number means.
DEFINITIONS = {
    'goal1_core_team_size':
        'The number of people who hold merge (write) access to the jsPsych '
        'repository as of the end of the window, taken from the maintained '
        'core-team roster in config/team.json rather than inferred from '
        'activity. A person is counted for a window if their merge access '
        'began on or before the window end and had not been revoked by then. '
        'This is a point-in-time headcount, not an activity measure: a core '
        'member is counted even if they made no contribution during the '
        'window.',
    'goal2_community_merge_time':
        'The median time, in days, from opening to merging a pull request '
        'authored by a community member (anyone NOT on the core-team roster), '
        'over pull requests merged within the trailing six-month window on the '
        'main jsPsych repository. Only merged PRs count toward the median; '
        'still-open or closed-unmerged PRs are excluded, as are PRs authored '
        'by core-team members (tracked separately) and by bots. Lower is '
        'better.',
    'goal3_support_response':
        'The median time, in days, from opening a discussion to its first '
        'reply by someone other than the author, over discussions opened '
        'within the trailing six-month window on the main jsPsych repository. '
        'Replies by the discussion author (self-replies) and by bots do not '
        'count as a response; a discussion is looked at across all later '
        'comments so an answer arriving after the window still counts. '
        'Discussions that never received a qualifying reply are excluded from '
        'the median. Lower is better.',
    'goal4_community_contributions':
        'The total number of contributions -- pull requests, issues, comments, '
        'and reviews -- made within the trailing six-month window across the '
        'community repositories (jspsych-contrib and jspsych-timelines). '
        'Bot-authored activity is excluded via the central bot filter. This is '
        'a volume measure of activity flowing into the wider jsPsych ecosystem; '
        'the companion unique-contributor count deduplicates people who are '
        'active in more than one community repository. Higher is better.',
    'goal5_new_contributors':
        'The headline figure is engagement-based: the number of people whose '
        'FIRST-EVER contribution of any kind to the main jsPsych repository -- '
        'authoring a pull request, opening an issue, leaving a comment, or '
        'submitting a review -- falls within the trailing six-month window. '
        'Each person is attributed to the window of their first-ever '
        'contribution across all history, so an established contributor active '
        'again in a later window is not re-counted as new; bot accounts are '
        'excluded. A second, commit-based figure is reported alongside it: the '
        'number of distinct GitHub accounts whose first non-merge commit on '
        'the default branch falls within the window. The commit-based figure '
        'matches GitHub\'s own contributor graph (merge commits and commits '
        'whose email is not linked to a GitHub account are excluded, as are '
        'bots) and is typically much smaller than the engagement-based figure, '
        'because many people participate through issues and discussion without '
        'ever landing a commit. Both are legitimate; they answer different '
        'questions and should never be conflated.',
}


def _quarter_start(dt: datetime) -> datetime:
    """Round a datetime down to the start of its calendar quarter (UTC)."""
    quarter_month = ((dt.month - 1) // 3) * 3 + 1
    return datetime(dt.year, quarter_month, 1, tzinfo=timezone.utc)


def _add_months(dt: datetime, months: int) -> datetime:
    """Return dt advanced by ``months`` calendar months (day pinned to 1)."""
    zero_based = (dt.month - 1) + months
    year = dt.year + zero_based // 12
    month = zero_based % 12 + 1
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _earliest_data_date(db_path: str, repo: Optional[str]) -> Optional[datetime]:
    """
    Find the earliest timestamp across all relevant tables.

    Args:
        db_path (str): Path to the SQLite database.
        repo (Optional[str]): Restrict to a single repo, or None for all repos.

    Returns:
        Optional[datetime]: The earliest UTC timestamp, or None if no data.
    """
    db = MetricsCalculator(db_path=db_path, days=None, repo=None).db
    earliest: Optional[pd.Timestamp] = None
    with db.get_connection() as conn:
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for table, column in _DATA_TABLES.items():
            if table not in existing:
                continue
            query = f"SELECT MIN({column}) FROM {table}"
            params: tuple = ()
            if repo is not None:
                query += " WHERE repo = ?"
                params = (repo,)
            value = conn.execute(query, params).fetchone()[0]
            if value is None:
                continue
            ts = pd.to_datetime(value, utc=True)
            if earliest is None or ts < earliest:
                earliest = ts
    if earliest is None:
        return None
    return earliest.to_pydatetime()


def rolling_windows(db_path: str, window_days: int = 182, step_months: int = 3,
                    repo: Optional[str] = None) -> Iterator[datetime]:
    """
    Yield rolling window-END datetimes stepped across history.

    Each yielded value is the *end* of a ``window_days``-long look-back window.
    Ends are quarter-aligned and advance by ``step_months`` from the quarter
    containing the earliest relevant data up to (and including) the most recent
    quarter start on/before now. A window whose start..end range lies entirely
    before any data simply produces empty/zero downstream rows; it is not an
    error.

    Args:
        db_path (str): Path to the SQLite database.
        window_days (int): Length of each look-back window in days (unused for
                           the step schedule itself; kept in the signature so
                           callers reason about windows in one place).
        step_months (int): Months to advance the window end between steps.
        repo (Optional[str]): Restrict the earliest-data scan to a single repo.

    Yields:
        datetime: Successive quarter-aligned window-end datetimes (UTC).
    """
    earliest = _earliest_data_date(db_path, repo)
    if earliest is None:
        return
    now = datetime.now(timezone.utc)
    cursor = _quarter_start(earliest)
    while cursor <= now:
        yield cursor
        cursor = _add_months(cursor, step_months)


def _window_contributor_logins(calc: MetricsCalculator) -> Set[str]:
    """
    Return the set of distinct contributor logins active in the calculator's
    window (PR authors, issue authors, commenters, reviewers).

    Reuses MetricsCalculator's filtered data access so the repo, bot, and
    date-window filters are applied consistently. Used by Goal 4 to compute a
    true union of contributors across the community repos.
    """
    logins: Set[str] = set()
    prs = calc._get_data_as_df('pull_requests', 'created_at')
    issues = calc._get_data_as_df('issues', 'created_at')
    comments = calc._get_data_as_df('comments', 'created_at')
    reviews = calc._get_data_as_df('reviews', 'submitted_at')
    if not prs.empty:
        logins |= set(prs['user_login'].dropna().unique())
    if not issues.empty:
        logins |= set(issues['user_login'].dropna().unique())
    if not comments.empty:
        logins |= set(comments['user_login'].dropna().unique())
    if not reviews.empty:
        logins |= set(reviews['reviewer_login'].dropna().unique())
    return logins


def goal_series(db_path: str = 'data/analytics.db', window_days: int = 182,
                step_months: int = 3) -> Dict[str, pd.DataFrame]:
    """
    Compute per-goal rolling time series.

    A single shared set of window ends (from ``rolling_windows`` over all repos)
    is used across every goal so the series line up for cross-goal comparison.
    Windows that predate a given goal's data yield zero/NaN rows rather than
    raising.

    Args:
        db_path (str): Path to the SQLite database.
        window_days (int): Look-back window length in days (default ~6 months).
        step_months (int): Months between successive window ends (default
                           quarterly). Use 6 for non-overlapping 6-month steps.

    Returns:
        Dict[str, pd.DataFrame]: Keyed by goal id (see module docstring), each a
        DataFrame indexed 0..n with a 'window_end' column plus the goal's
        metric columns.
    """
    windows = list(rolling_windows(db_path, window_days=window_days,
                                   step_months=step_months, repo=None))

    community_repos = repos.repo_names(role='community')

    goal1_rows = []
    goal2_rows = []
    goal3_rows = []
    goal4_rows = []

    for window_end in windows:
        # --- Goal 1: contributors with merge access (roster-based) ---
        goal1_rows.append({
            'window_end': window_end,
            'value': team.core_team_size(at_date=window_end),
        })

        # --- Goals 2 & 3: main-repo PR review/merge + support metrics ---
        main_calc = MetricsCalculator(db_path=db_path, days=window_days,
                                      repo=repos.MAIN_REPO, end_date=window_end)

        merge_by_group = main_calc.get_median_pr_merge_time_by_group()
        rate_by_group = main_calc.get_pr_merge_rate_by_group()
        community = rate_by_group['community']
        core = rate_by_group['core']
        goal2_rows.append({
            'window_end': window_end,
            'community_median_days': (
                merge_by_group['community']
                if community['total_merged'] > 0 else float('nan')
            ),
            'core_median_days': (
                merge_by_group['core'] if core['total_merged'] > 0 else float('nan')
            ),
            'community_merge_rate': (
                community['merge_rate'] if community['total_prs'] > 0 else float('nan')
            ),
        })

        answer = main_calc.get_discussion_answer_rate()
        share = main_calc.get_response_share()
        goal3_rows.append({
            'window_end': window_end,
            'median_first_response_days': (
                main_calc.get_med_time_to_first_discussion_response()
                if answer['total_discussions'] > 0 else float('nan')
            ),
            'qa_answer_rate': (
                answer['qa_answer_rate'] if answer['qa_total'] > 0 else float('nan')
            ),
            'core_response_share_pct': (
                share['core_team_pct'] if share['total_responses'] > 0 else float('nan')
            ),
        })

        # --- Goal 4: contribution rate across community repos ---
        contributions = 0
        contributor_logins: Set[str] = set()
        for repo in community_repos:
            repo_calc = MetricsCalculator(db_path=db_path, days=window_days,
                                          repo=repo, end_date=window_end)
            summary = repo_calc.get_contributor_summary()
            contributions += summary['total_contributions']
            contributor_logins |= _window_contributor_logins(repo_calc)
        goal4_rows.append({
            'window_end': window_end,
            'contributions': contributions,
            'unique_contributors': len(contributor_logins),
        })

    # --- Goal 5: new (first-ever) contributors to the main repo per window ---
    # Compute first-ever contribution/commit dates ONCE over all history, then
    # bucket into windows with pandas -- avoids re-querying per window. Two
    # definitions run in parallel: engagement-based (PR/issue/comment/review
    # authors, the headline) and commit-based (non-merge commit authors, the
    # GitHub-contributor-graph definition).
    goal5_calc = MetricsCalculator(db_path=db_path, days=None, repo=repos.MAIN_REPO)
    first_contribution = goal5_calc._first_contribution_dates()
    first_commit = goal5_calc._first_commit_dates()
    goal5_rows = []
    for window_end in windows:
        window_start = window_end - pd.Timedelta(days=window_days)

        def _count_in_window(first_dates):
            if first_dates.empty:
                return 0
            in_window = (
                (first_dates >= window_start)
                & (first_dates < window_end)
            )
            return int(in_window.sum())

        goal5_rows.append({
            'window_end': window_end,
            'new_contributors': _count_in_window(first_contribution),
            'new_commit_contributors': _count_in_window(first_commit),
        })

    return {
        'goal1_core_team_size': pd.DataFrame(
            goal1_rows, columns=['window_end', 'value']),
        'goal2_community_merge_time': pd.DataFrame(
            goal2_rows,
            columns=['window_end', 'community_median_days', 'core_median_days',
                     'community_merge_rate']),
        'goal3_support_response': pd.DataFrame(
            goal3_rows,
            columns=['window_end', 'median_first_response_days', 'qa_answer_rate',
                     'core_response_share_pct']),
        'goal4_community_contributions': pd.DataFrame(
            goal4_rows,
            columns=['window_end', 'contributions', 'unique_contributors']),
        'goal5_new_contributors': pd.DataFrame(
            goal5_rows,
            columns=['window_end', 'new_contributors', 'new_commit_contributors']),
    }


def current_vs_previous(series_df: pd.DataFrame, value_col: str) -> dict:
    """
    Compare the latest window to the window two quarters (6 months) earlier.

    With the default quarterly step this compares the most recent window to the
    one two rows back, i.e. a NON-overlapping 6-month-ago window (rows one step
    back would overlap the 6-month look-back and so are skipped).

    Args:
        series_df (pd.DataFrame): A goal series (must contain ``value_col``,
                                  ordered oldest-to-newest as produced by
                                  goal_series).
        value_col (str): The metric column to compare.

    Returns:
        dict: {'current', 'previous', 'change_pct'}. ``change_pct`` is None when
              there are too few windows or the previous/current value is
              missing or the previous value is zero (division guard).
    """
    values = list(series_df[value_col]) if value_col in series_df.columns else []
    current = values[-1] if len(values) >= 1 else None
    previous = values[-3] if len(values) >= 3 else None

    def _clean(v):
        return None if v is None or pd.isna(v) else float(v)

    current = _clean(current)
    previous = _clean(previous)

    if current is None or previous is None or previous == 0:
        change_pct = None
    else:
        change_pct = (current - previous) / previous * 100.0

    return {'current': current, 'previous': previous, 'change_pct': change_pct}
