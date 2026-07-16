"""
Calculates repository metrics based on data in the SQLite database.

This module provides a MetricsCalculator class that can be instantiated for
different time periods (e.g., 30, 60, 90 days, or all time) to compute
a wide range of metrics related to library health, pull requests, issues, and community
engagement.
"""



from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import numpy as np
import pandas as pd

from . import repos, team
from .database import DatabaseManager

# Maps table name -> author login column used for bot filtering. Tables not
# listed here (e.g. releases) are not filtered: a release published by a bot is
# still a real release.
_AUTHOR_COLUMNS = {
    'pull_requests': 'user_login',
    'issues': 'user_login',
    'comments': 'user_login',
    'reviews': 'reviewer_login',
    'discussions': 'author_login',
    'discussion_comments': 'author_login',
    # Commits carry an author_login that is NULL when the commit email is not
    # linked to a GitHub account. team.is_bot(None) returns False, so those
    # NULL-login rows are KEPT by the central bot filter (they are real
    # commits); the commit-contributor metrics exclude them separately because
    # a NULL login cannot be counted as a distinct GitHub contributor.
    'commits': 'author_login',
}


class MetricsCalculator:
    """
    A class to calculate various repository metrics from a database.
    
    Attributes:
        db (DatabaseManager): An instance of the database manager.
        days (Optional[int]): The number of days to look back for metrics. If None, calculates for all time.
        end_date (datetime): The end date for the time window. Defaults to the
                             current time; can be set to an earlier date to
                             "time-travel" and evaluate a historical window.
        start_date (Optional[datetime]): The start date for the time window.
    """
    def __init__(self, db_path: str = "../data/analytics.db", days: Optional[int] = 30,
                 repo: Optional[str] = repos.MAIN_REPO,
                 end_date: Optional[object] = None):
        """
        Initializes the MetricsCalculator.

        Args:
            db_path (str): The path to the SQLite database file.
            days (Optional[int]): The number of days for the metrics window.
                                  Pass None for "all time" metrics.
            repo (Optional[str]): Restrict metrics to a single repository
                                  ("owner/name"). Defaults to the main repo so
                                  existing dashboard pages stay jsPsych-only.
                                  Pass None to include all repositories.
            end_date (Optional[datetime | str]): The upper bound of the time
                                  window, as a datetime or ISO-8601 string.
                                  Default None means "now" and preserves the
                                  original behavior exactly (no upper-bound
                                  filter, since nothing is dated in the
                                  future). When provided it is coerced to a
                                  timezone-aware UTC datetime, and
                                  _get_data_as_df additionally filters rows to
                                  date_column <= end_date, so metrics reflect
                                  the state "as of" that date. Rolling
                                  evaluation windows use this to step across
                                  history.
        """
        self.db = DatabaseManager(db_path)
        self.days = days
        self.repo = repo

        # Track whether an explicit end_date was supplied. Only then do we
        # apply an upper-bound date filter; with end_date=None nothing is in
        # the future, so today's behavior (no upper bound) is preserved.
        self._end_date_explicit = end_date is not None
        if end_date is None:
            self.end_date = datetime.now(timezone.utc)
        else:
            self.end_date = self._coerce_utc(end_date)

        if self.days is not None:
            self.start_date = self.end_date - timedelta(days=self.days)
        else:
            self.start_date = None # For "all time" calculations

    @staticmethod
    def _coerce_utc(value: object) -> datetime:
        """Coerce a datetime or ISO-8601 string into a tz-aware UTC datetime."""
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _get_data_as_df(self, table_name: str, date_column: str = 'created_at',
                         ignore_time_filter: bool = False) -> pd.DataFrame:
        """
        Fetch data from a table within the time window and return as a pandas DataFrame.

        Args:
            table_name (str): The name of the database table.
            date_column (str): The name of the date column to filter on.
            ignore_time_filter (bool): If True, skip the self.days/start_date
                                       window and return all-time data (still
                                       subject to the repo and bot filters).
                                       Used by metrics that need full history
                                       regardless of the calculator's window,
                                       e.g. determining a contributor's
                                       first-ever contribution.

        Returns:
            pd.DataFrame: A DataFrame containing the filtered data.
        """
        with self.db.get_connection() as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return pd.DataFrame()

        # Restrict to a single repository (unless repo is None = all repos).
        # Applied before the bot/time filters so downstream logic is unchanged.
        if self.repo is not None and 'repo' in df.columns:
            df = df[df['repo'] == self.repo].copy()
            if df.empty:
                return pd.DataFrame()

        # Centrally drop bot-authored rows. The author column varies by table;
        # tables not in _AUTHOR_COLUMNS (e.g. releases) are left unfiltered.
        author_column = _AUTHOR_COLUMNS.get(table_name)
        if author_column and author_column in df.columns:
            # A SQL NULL login (e.g. a commit whose author email is not linked
            # to a GitHub account) reads back as a float NaN; normalize it to
            # None so team.is_bot -- which treats a missing login as "not a
            # bot" -- KEEPS the row rather than raising on NaN.endswith(...).
            def _login_or_none(value):
                return None if pd.isna(value) else value

            # user_type is only present on pull_requests and issues.
            if 'user_type' in df.columns:
                is_bot_mask = pd.Series(
                    [team.is_bot(_login_or_none(login), user_type)
                     for login, user_type
                     in zip(df[author_column], df['user_type'])],
                    index=df.index,
                )
            else:
                is_bot_mask = pd.Series(
                    [team.is_bot(_login_or_none(login)) for login in df[author_column]],
                    index=df.index,
                )
            df = df[~is_bot_mask].copy()
            if df.empty:
                return pd.DataFrame()

        # Lower bound: the rolling start_date, unless the caller asked for all
        # history (ignore_time_filter) or this is an all-time calculator.
        apply_lower = (
            self.start_date is not None
            and not ignore_time_filter
        )
        # Upper bound: only when an explicit end_date was supplied. This caps
        # the calculator's view of the database at end_date, so even "all
        # history" (ignore_time_filter) queries see the database as of that
        # date -- e.g. a discussion's first response after end_date is not
        # counted. With end_date=None this branch never runs, so today's
        # behavior is unchanged.
        apply_upper = self._end_date_explicit

        if (apply_lower or apply_upper) and date_column in df.columns:
            df = df.copy()
            df[date_column] = pd.to_datetime(df[date_column], utc=True)
            if apply_lower:
                df = df[df[date_column] >= self.start_date]
            if apply_upper:
                df = df[df[date_column] <= self.end_date]
            return df.copy()

        return df

    def _author_group(self, series: pd.Series) -> pd.Series:
        """
        Label each login as 'core' or 'community' based on merge access.

        A login is 'core' if it is a current core-team member (per
        team.core_team_logins()), otherwise 'community'. Used to split
        author-oriented PR metrics by contributor group for Goal 2.

        Args:
            series (pd.Series): A Series of author logins.

        Returns:
            pd.Series: A Series of 'core'/'community' labels aligned to series.
        """
        core_logins = team.core_team_logins()
        return series.apply(lambda login: 'core' if login in core_logins else 'community')

    # overview metrics
    # -- valueboxes --
    def get_active_items(self) -> Dict[str, int]:
        """
        Get the count of active pull requests and issues.

        Caveat: this reflects the *current* stored state (rows whose state is
        'open'), not the state as of the window. Setting end_date does not make
        this time-travel -- an item open today is counted even for a historical
        end_date, and one created after end_date is still excluded by the
        upper-bound date filter. Treat this as a "current backlog" metric.

        Returns:
            Dict[str, int]: A dictionary with counts of open PRs, open issues, and total active items.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        issues_df = self._get_data_as_df('issues', 'created_at')

        open_prs = prs_df[prs_df['state'] == 'open'].shape[0] if not prs_df.empty else 0
        open_issues = issues_df[issues_df['state'] == 'open'].shape[0] if not issues_df.empty else 0

        return {
            "open_prs": open_prs,
            "open_issues": open_issues,
            "total": open_prs + open_issues
        }
    
    def get_community_engagement(self) -> Dict[str, int]:
        """
        Get the count of total engagement (comments, reviews, issues, PRs) by community members 
        and also total unique contributors.
    
        Returns:
            Dict[str, int]: A dictionary containing:
                - unique_contributors: Number of unique users who have contributed
                - total_engagements: Total number of interactions (comments, reviews, issues, PRs)
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        issues_df = self._get_data_as_df('issues', 'created_at')
        comments_df = self._get_data_as_df('comments', 'created_at')
        reviews_df = self._get_data_as_df('reviews', 'submitted_at')

        pr_authors = set()
        issue_authors = set()
        commenters = set()
        reviewers = set()

        if not prs_df.empty:
            pr_authors = set(prs_df['user_login'].unique())

        if not issues_df.empty:
            issue_authors = set(issues_df['user_login'].unique())
    
        if not comments_df.empty:
            commenters = set(comments_df['user_login'].unique())
    
        if not reviews_df.empty:
            reviewers = set(reviews_df['reviewer_login'].unique())
        
        all_contributors = pr_authors | issue_authors | commenters | reviewers
        
        total_engagements = (
            len(prs_df) +
            len(issues_df) +
            len(comments_df) +
            len(reviews_df)
        )
        
        return {
            "unique_contributors": len(all_contributors),
            "total_engagements": total_engagements
        }


    def get_core_team_size(self) -> int:
        """
        Get the current number of contributors with merge access (the core team).

        This is the Goal 1 sustainability metric, sourced from the maintained
        roster in config/team.json.

        Returns:
            int: The current core team size.
        """
        return team.core_team_size()

    def get_throughput(self) -> int:
        """
        Get the throughput (number of issues closed and PRs merged) in the given time period.

        Returns:
            int: The total throughput (closed issues + merged PRs).
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        issues_df = self._get_data_as_df('issues', 'created_at')

        closed_issues = issues_df[issues_df['state'] == 'closed'].shape[0] if not issues_df.empty else 0
        merged_prs = prs_df[prs_df['state'] == 'merged'].shape[0] if not prs_df.empty else 0

        return closed_issues + merged_prs
    
    def get_total_releases(self) -> dict[str, int]:
        """
        Get the total number of releases in the given time period.

        Returns:
            dict: A dictionary with total releases and churn (merged PRs).
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        releases_df = self._get_data_as_df('releases', 'created_at')
        churn = prs_df[prs_df['state'] == 'merged'].shape[0] if not prs_df.empty else 0
        return {
            "releases": releases_df.shape[0] if not releases_df.empty else 0,
            "churn": churn
        }
    
    # -- charts -- 

    def get_backlog_trend(self) -> pd.DataFrame:
        """
        Get the backlog trend over time for both issues and pull requests.

        The backlog is the cumulative sum of newly opened items minus newly closed/merged items per day.

        Returns:
            pd.DataFrame: A DataFrame with columns ['date', 'opened', 'closed', 'backlog'].
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        issues_df = self._get_data_as_df('issues', 'created_at')
        if prs_df.empty and issues_df.empty:
            return pd.DataFrame(columns=['date', 'opened', 'closed', 'backlog'])

        opened_dates = pd.concat([
            pd.to_datetime(prs_df['created_at'], utc=True),
            pd.to_datetime(issues_df['created_at'], utc=True)
        ]).dt.floor('D') # Floor to the day

        closed_pr_dates = prs_df[prs_df['state'].isin(['closed', 'merged'])]['updated_at']
        closed_issue_dates = issues_df[issues_df['state'] == 'closed']['closed_at']
        
        closed_dates = pd.concat([
            pd.to_datetime(closed_pr_dates, utc=True),
            pd.to_datetime(closed_issue_dates, utc=True)
        ]).dt.floor('D') # Floor to the day
        
        opened_counts = opened_dates.value_counts().reset_index()
        opened_counts.columns = ['date', 'opened']
        
        closed_counts = closed_dates.value_counts().reset_index()
        closed_counts.columns = ['date', 'closed']
        
        trend_df = pd.merge(opened_counts, closed_counts, on='date', how='outer').fillna(0)
        trend_df = trend_df.sort_values('date').reset_index(drop=True)
        
        trend_df['net_change'] = trend_df['opened'] - trend_df['closed']
        trend_df['backlog'] = trend_df['net_change'].cumsum()
        
        return trend_df[['date', 'opened', 'closed', 'backlog']]
        
    def get_code_churn(self) -> pd.DataFrame:
        """
        Get code churn metrics (lines added, lines deleted) over time.

        Returns:
            pd.DataFrame: A DataFrame with code churn metrics.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        if prs_df.empty or 'additions' not in prs_df.columns or 'deletions' not in prs_df.columns:
            return pd.DataFrame(columns=['date', 'additions', 'deletions'])
        prs_df['date'] = pd.to_datetime(prs_df['created_at']).dt.date
        churn = prs_df.groupby('date').agg({'additions': 'sum', 'deletions': 'sum'}).reset_index()
        return churn

    def get_release_and_pr_timeline(self) -> pd.DataFrame:
        """
        Get a dataframe with releases and PRs merged during that release period
        
        Returns:
            pd.DataFrame: A DataFrame with releases and associated PRs.
        """
        releases_df = self._get_data_as_df('releases', 'created_at')
        prs_df = self._get_data_as_df('pull_requests', 'merged_at')
        if releases_df.empty or prs_df.empty:
            return pd.DataFrame(columns=['release', 'release_date', 'merged_pr_count'])
        releases_df = releases_df.sort_values('created_at')
        prs_df = prs_df[prs_df['state'] == 'merged']
        result = []
        for i, row in releases_df.iterrows():
            start = row['created_at']
            end = releases_df.iloc[i+1]['created_at'] if i+1 < len(releases_df) else self.end_date.isoformat()
            merged_count = prs_df[(prs_df['merged_at'] >= start) & (prs_df['merged_at'] < end)].shape[0]
            result.append({'release': row.get('name', f'Release {i+1}'), 'release_date': start, 'merged_pr_count': merged_count})
        return pd.DataFrame(result)

    

    # pr metrics
    # -- valueboxes --

    def get_median_pr_merge_time(self) -> float:
        """
        Get the median time to merge pull requests in the given time period.

        Returns:
            float: The median time to merge PRs in days.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        if prs_df.empty or 'merged_at' not in prs_df.columns:
            return 0.0
        merged = prs_df[prs_df['state'] == 'merged'].copy()
        if merged.empty:
            return 0.0
        merged['created_at'] = pd.to_datetime(merged['created_at'], utc=True)
        merged['merged_at'] = pd.to_datetime(merged['merged_at'], utc=True)
        merged['merge_time'] = (merged['merged_at'] - merged['created_at']).dt.total_seconds() / 86400
        return float(merged['merge_time'].median())
 
    def get_med_time_to_first_response_prs(self) -> float:
        """
        Get the median time to first response (comment or review) on pull requests. Bot-authored comments and reviews are excluded via the central bot filter.

        Returns:
            float: The median time to first response in days.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        comments_df = self._get_data_as_df('comments', 'created_at')
        reviews_df = self._get_data_as_df('reviews', 'submitted_at')
        if prs_df.empty:
            return 0.0

        times = []

        for _, pr in prs_df.iterrows():
            pr_created = pd.to_datetime(pr['created_at'], utc=True)
            pr_number = pr['number']

            first_comment_time = None
            if not comments_df.empty:
                pr_comments = comments_df[comments_df['pr_number'] == pr_number]
                if not pr_comments.empty:
                    first_comment_time = pd.to_datetime(pr_comments['created_at'].min(), utc=True)

            first_review_time = None
            if not reviews_df.empty:
                pr_reviews = reviews_df[reviews_df['pr_number'] == pr_number]
                if not pr_reviews.empty:
                    first_review_time = pd.to_datetime(pr_reviews['submitted_at'].min(), utc=True)

            firsts = [t for t in [first_comment_time, first_review_time] if t is not None]
            if firsts:
                first_response = min(firsts)
                delta_days = (first_response - pr_created).total_seconds() / 86400
                times.append(delta_days)

        return float(np.median(times)) if times else 0.0
    
    def get_pr_merge_rate(self) -> dict:
        """
        Get the pull request merge rate in the given time period.

        Returns:
            dict: A dictionary with merge rate percentage, total merged PRs, and total PRs.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        if prs_df.empty:
            return {"merge_rate": 0.0, "total_merged": 0, "total_prs": 0}
        merged = prs_df[prs_df['state'] == 'merged']
        return {
            "merge_rate": 100.0 * len(merged) / len(prs_df) if len(prs_df) > 0 else 0.0,
            "total_merged": len(merged),
            "total_prs": len(prs_df)
        }
    
    def get_median_pr_merge_time_by_group(self) -> Dict[str, float]:
        """
        Get the median PR merge time split by author group (Goal 2).

        Merged PRs in the current time window are split by whether their
        author is a core-team member ('core') or not ('community'). This
        isolates how long *community* contributions wait to merge from
        core-team self-merges.

        Returns:
            Dict[str, float]: {'core': median_days, 'community': median_days}.
                              A group with no merged PRs maps to 0.0.
        """
        result = {'core': 0.0, 'community': 0.0}
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        if prs_df.empty or 'merged_at' not in prs_df.columns:
            return result
        merged = prs_df[prs_df['state'] == 'merged'].copy()
        if merged.empty:
            return result
        merged['created_at'] = pd.to_datetime(merged['created_at'], utc=True)
        merged['merged_at'] = pd.to_datetime(merged['merged_at'], utc=True)
        merged['merge_time'] = (merged['merged_at'] - merged['created_at']).dt.total_seconds() / 86400
        merged['group'] = self._author_group(merged['user_login'])
        for group, sub in merged.groupby('group'):
            result[group] = float(sub['merge_time'].median())
        return result

    def get_med_time_to_first_response_prs_by_group(self) -> Dict[str, float]:
        """
        Get the median time to first response on PRs split by author group (Goal 2).

        Uses the same per-PR first-response logic as
        get_med_time_to_first_response_prs (first comment or review on the
        PR, with bot authors excluded via the central filter), but groups the
        resulting per-PR response times by the PR *author's* group. The
        question this answers is "how fast do community authors get a first
        response versus core-team authors?".

        Returns:
            Dict[str, float]: {'core': median_days, 'community': median_days}.
                              A group with no responded PRs maps to 0.0.
        """
        result = {'core': 0.0, 'community': 0.0}
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        comments_df = self._get_data_as_df('comments', 'created_at')
        reviews_df = self._get_data_as_df('reviews', 'submitted_at')
        if prs_df.empty:
            return result

        core_logins = team.core_team_logins()
        times = {'core': [], 'community': []}

        for _, pr in prs_df.iterrows():
            pr_created = pd.to_datetime(pr['created_at'], utc=True)
            pr_number = pr['number']

            first_comment_time = None
            if not comments_df.empty:
                pr_comments = comments_df[comments_df['pr_number'] == pr_number]
                if not pr_comments.empty:
                    first_comment_time = pd.to_datetime(pr_comments['created_at'].min(), utc=True)

            first_review_time = None
            if not reviews_df.empty:
                pr_reviews = reviews_df[reviews_df['pr_number'] == pr_number]
                if not pr_reviews.empty:
                    first_review_time = pd.to_datetime(pr_reviews['submitted_at'].min(), utc=True)

            firsts = [t for t in [first_comment_time, first_review_time] if t is not None]
            if firsts:
                delta_days = (min(firsts) - pr_created).total_seconds() / 86400
                group = 'core' if pr['user_login'] in core_logins else 'community'
                times[group].append(delta_days)

        for group, values in times.items():
            if values:
                result[group] = float(np.median(values))
        return result

    def get_pr_merge_rate_by_group(self) -> dict:
        """
        Get the PR merge rate split by author group (Goal 2).

        PRs in the current time window are split by whether their author is a
        core-team member ('core') or not ('community'), so community merge
        acceptance can be tracked separately from core-team self-merges.

        Returns:
            dict: {'core': {merge_rate, total_merged, total_prs},
                   'community': {merge_rate, total_merged, total_prs}}.
                  A group with no PRs keeps the zeroed default.
        """
        result = {
            'core': {"merge_rate": 0.0, "total_merged": 0, "total_prs": 0},
            'community': {"merge_rate": 0.0, "total_merged": 0, "total_prs": 0},
        }
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        if prs_df.empty:
            return result
        prs_df = prs_df.copy()
        prs_df['group'] = self._author_group(prs_df['user_login'])
        for group, sub in prs_df.groupby('group'):
            merged = sub[sub['state'] == 'merged']
            total_prs = len(sub)
            result[group] = {
                "merge_rate": 100.0 * len(merged) / total_prs if total_prs > 0 else 0.0,
                "total_merged": len(merged),
                "total_prs": total_prs,
            }
        return result

    def backlog_trend_prs(self) -> int:
        """
        Get the open pull requests count.

        Returns:
            int: The backlog value.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        return prs_df[prs_df['state'] == 'open'].shape[0] if not prs_df.empty else 0

    # -- charts --
    def merge_time_distribution(self) -> pd.DataFrame:
        """
        Get the distribution of merge times for pull requests.

        Returns:
            pd.DataFrame: A DataFrame with merge time distribution data.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        if prs_df.empty or 'merged_at' not in prs_df.columns:
            return pd.DataFrame(columns=['merge_time_days'])
        merged = prs_df[prs_df['state'] == 'merged'].copy()
        if merged.empty:
            return pd.DataFrame(columns=['merge_time_days'])
        merged['created_at'] = pd.to_datetime(merged['created_at'], utc=True)
        merged['merged_at'] = pd.to_datetime(merged['merged_at'], utc=True)
        merged['merge_time_days'] = (merged['merged_at'] - merged['created_at']).dt.total_seconds() / 86400
        return merged[['merge_time_days']]
    
    def pr_size_distribution(self) -> pd.DataFrame:
        """
        Get the distribution of pull request sizes (lines changed).

        Returns:
            pd.DataFrame: A DataFrame with PR size distribution data.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        if prs_df.empty or 'additions' not in prs_df.columns or 'deletions' not in prs_df.columns:
            return pd.DataFrame(columns=['size'])
        prs_df['size'] = prs_df['additions'] + prs_df['deletions']
        return prs_df[['size']]

    def get_pr_merge_time_trend(self, freq: str = 'QE') -> pd.DataFrame:
        """
        Get the median PR merge time per period, split by author group (Goal 2 trend).

        Trends span the full history of the repo regardless of self.days, so
        six-month evaluations can ask "is this improving?". Merged PRs are
        bucketed by their merged_at date and the median merge time (in days)
        is computed for core authors, community authors, and all authors.

        Args:
            freq (str): The pandas frequency alias to bucket periods by
                        (e.g. 'QE' for quarter-end). Defaults to quarterly.

        Returns:
            pd.DataFrame: A DataFrame with columns ['period', 'core',
                          'community', 'all']. Periods with no merged PRs are
                          omitted; within a kept period a group with no merged
                          PRs has a NaN value for that column.
        """
        columns = ['period', 'core', 'community', 'all']
        prs_df = self._get_data_as_df('pull_requests', 'created_at', ignore_time_filter=True)
        if prs_df.empty or 'merged_at' not in prs_df.columns:
            return pd.DataFrame(columns=columns)
        merged = prs_df[prs_df['state'] == 'merged'].copy()
        if merged.empty:
            return pd.DataFrame(columns=columns)
        merged['created_at'] = pd.to_datetime(merged['created_at'], utc=True)
        merged['merged_at'] = pd.to_datetime(merged['merged_at'], utc=True)
        merged['merge_time'] = (merged['merged_at'] - merged['created_at']).dt.total_seconds() / 86400
        merged['group'] = self._author_group(merged['user_login'])

        grouper = pd.Grouper(key='merged_at', freq=freq)
        all_med = merged.groupby(grouper)['merge_time'].median()
        core_med = merged[merged['group'] == 'core'].groupby(grouper)['merge_time'].median()
        community_med = merged[merged['group'] == 'community'].groupby(grouper)['merge_time'].median()

        result = pd.DataFrame({'core': core_med, 'community': community_med, 'all': all_med})
        result = result.dropna(subset=['all'])
        result = result.reset_index().rename(columns={'merged_at': 'period'})
        return result[columns]

    def get_pr_first_response_trend(self, freq: str = 'QE') -> pd.DataFrame:
        """
        Get the median time to first response for PRs created per period (Goal 2 trend).

        Trends span the full history of the repo regardless of self.days. Each
        PR is bucketed by its created_at date and mapped to its time-to-first
        response (first comment or review, bots excluded via the central
        filter, identical semantics to get_med_time_to_first_response_prs);
        the median is taken per period. All authors are included.

        Args:
            freq (str): The pandas frequency alias to bucket periods by
                        (e.g. 'QE' for quarter-end). Defaults to quarterly.

        Returns:
            pd.DataFrame: A DataFrame with columns ['period', 'median_days'].
        """
        columns = ['period', 'median_days']
        prs_df = self._get_data_as_df('pull_requests', 'created_at', ignore_time_filter=True)
        if prs_df.empty:
            return pd.DataFrame(columns=columns)
        comments_df = self._get_data_as_df('comments', 'created_at', ignore_time_filter=True)
        reviews_df = self._get_data_as_df('reviews', 'submitted_at', ignore_time_filter=True)

        records = []
        for _, pr in prs_df.iterrows():
            pr_created = pd.to_datetime(pr['created_at'], utc=True)
            pr_number = pr['number']

            first_comment_time = None
            if not comments_df.empty:
                pr_comments = comments_df[comments_df['pr_number'] == pr_number]
                if not pr_comments.empty:
                    first_comment_time = pd.to_datetime(pr_comments['created_at'].min(), utc=True)

            first_review_time = None
            if not reviews_df.empty:
                pr_reviews = reviews_df[reviews_df['pr_number'] == pr_number]
                if not pr_reviews.empty:
                    first_review_time = pd.to_datetime(pr_reviews['submitted_at'].min(), utc=True)

            firsts = [t for t in [first_comment_time, first_review_time] if t is not None]
            if firsts:
                delta_days = (min(firsts) - pr_created).total_seconds() / 86400
                records.append({'created_at': pr_created, 'response_days': delta_days})

        if not records:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(records)
        result = (
            df.groupby(pd.Grouper(key='created_at', freq=freq))['response_days']
            .median()
            .reset_index(name='median_days')
            .rename(columns={'created_at': 'period'})
        )
        result = result.dropna(subset=['median_days'])
        return result[columns]

    # issue metrics
    # -- valueboxes --
    def get_median_issue_close_time(self) -> float:
        """
        Get the median time to close issues in the given time period.

        Returns:
            float: The median time to close issues in days.
        """
        issues_df = self._get_data_as_df('issues', 'created_at')
        if issues_df.empty or 'closed_at' not in issues_df.columns:
            return 0.0
        closed = issues_df[issues_df['state'] == 'closed'].copy()
        if closed.empty:
            return 0.0
        closed['created_at'] = pd.to_datetime(closed['created_at'], utc=True)
        closed['closed_at'] = pd.to_datetime(closed['closed_at'], utc=True)
        closed['close_time'] = (closed['closed_at'] - closed['created_at']).dt.total_seconds() / 86400
        return float(closed['close_time'].median())
    def get_time_to_first_response_issue(self) -> float:
        """
        Get the median time to first response (comment) on issues.

        Returns:
            float: The median time to first response in days.
        """
        issues_df = self._get_data_as_df('issues', 'created_at')
        comments_df = self._get_data_as_df('comments', 'created_at')
        if issues_df.empty:
            return 0.0
        times = []
        for _, issue in issues_df.iterrows():
            issue_created = pd.to_datetime(issue['created_at'], utc=True)
            issue_number = issue['number'] if 'number' in issue else issue.get('id')
            if not comments_df.empty and 'issue_number' in comments_df.columns:
                issue_comments = comments_df[comments_df['issue_number'] == issue_number]
                if not issue_comments.empty:
                    first_comment_time = pd.to_datetime(issue_comments['created_at'].min(), utc=True)
                    times.append((first_comment_time - issue_created).total_seconds() / 86400)
        return float(np.median(times)) if times else 0.0
    
    def get_issue_close_rate(self) -> dict:
        """
        Get the issue close rate in the given time period.

        Returns:
            dict: A dictionary with close rate percentage, total closed issues, and total issues.
        """
        issues_df = self._get_data_as_df('issues', 'created_at')
        if issues_df.empty:
            return {"close_rate": 0.0, "total_closed": 0, "total_issues": 0}
        closed = issues_df[issues_df['state'] == 'closed']
        return {
            "close_rate": 100.0 * len(closed) / len(issues_df) if len(issues_df) > 0 else 0.0,
            "total_closed": len(closed),
            "total_issues": len(issues_df)
        }
    def backlog_trend_issues(self) -> int:
        """
        Get the open issues count.

        Returns:
            int: The backlog value.
        """
        issues_df = self._get_data_as_df('issues', 'created_at')
        return issues_df[issues_df['state'] == 'open'].shape[0] if not issues_df.empty else 0
    # -- charts --
    def open_issues_aging(self) -> pd.DataFrame:
        """
        Get the aging distribution of open issues.

        Caveat: "open" is the issue's *current* stored state, so this cannot
        truly time-travel. With an explicit end_date the age is measured
        against end_date and issues created after end_date are dropped, but an
        issue that is open today is treated as open for the whole window even
        if it was actually closed later than end_date. Treat this as a
        current-state metric evaluated as of end_date.

        Returns:
            pd.DataFrame: A DataFrame with open issues aging data.
        """
        issues_df = self._get_data_as_df('issues', 'created_at')
        if issues_df.empty:
            return pd.DataFrame(columns=['age_days'])
        open_issues = issues_df[issues_df['state'] == 'open'].copy()
        if open_issues.empty:
            return pd.DataFrame(columns=['age_days'])
        open_issues['created_at'] = pd.to_datetime(open_issues['created_at'], utc=True)
        open_issues['age_days'] = (self.end_date - open_issues['created_at']).dt.total_seconds() / 86400
        return open_issues[['age_days']]
    def open_issues_type(self) -> pd.DataFrame:
        """
        Get the distribution of open issues by type (bug, feature, etc.).

        Returns:
            pd.DataFrame: A DataFrame with open issues type data.
        """
        issues_df = self._get_data_as_df('issues', 'created_at')
        if issues_df.empty or 'issue_type' not in issues_df.columns:
            return pd.DataFrame(columns=['type', 'count'])
        open_issues = issues_df[issues_df['state'] == 'open']
        return open_issues['issue_type'].value_counts().reset_index().rename(columns={'index': 'type', 'type': 'count'})

    # community / contribution metrics (Goal 4: rate of contribution to
    # community repositories)
    # -- helpers --

    def _first_contribution_dates(self) -> pd.Series:
        """
        Get each contributor's first-ever contribution timestamp.

        Considers PRs, issues, comments, and reviews, and looks across the
        full history of the repo (ignoring self.days) so that a narrow time
        window doesn't make every active contributor look "new".

        Returns:
            pd.Series: Series indexed by login, with the earliest
                       contribution timestamp (UTC) as the value. Empty if
                       there is no data.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at', ignore_time_filter=True)
        issues_df = self._get_data_as_df('issues', 'created_at', ignore_time_filter=True)
        comments_df = self._get_data_as_df('comments', 'created_at', ignore_time_filter=True)
        reviews_df = self._get_data_as_df('reviews', 'submitted_at', ignore_time_filter=True)

        frames = []
        if not prs_df.empty:
            frames.append(prs_df[['user_login', 'created_at']].rename(
                columns={'user_login': 'login', 'created_at': 'timestamp'}))
        if not issues_df.empty:
            frames.append(issues_df[['user_login', 'created_at']].rename(
                columns={'user_login': 'login', 'created_at': 'timestamp'}))
        if not comments_df.empty:
            frames.append(comments_df[['user_login', 'created_at']].rename(
                columns={'user_login': 'login', 'created_at': 'timestamp'}))
        if not reviews_df.empty:
            frames.append(reviews_df[['reviewer_login', 'submitted_at']].rename(
                columns={'reviewer_login': 'login', 'submitted_at': 'timestamp'}))

        if not frames:
            return pd.Series(dtype='datetime64[ns, UTC]')

        all_activity = pd.concat(frames, ignore_index=True)
        all_activity['timestamp'] = pd.to_datetime(all_activity['timestamp'], utc=True)
        return all_activity.groupby('login')['timestamp'].min()

    # -- valueboxes / charts --

    def get_contribution_rate(self, freq: str = 'QE') -> pd.DataFrame:
        """
        Get the count of merged pull requests and opened issues per period.

        This is the Goal 4 sustainability metric: the rate of contribution
        to community repositories over time.

        Args:
            freq (str): The pandas frequency alias to bucket periods by
                        (e.g. 'QE' for quarter-end). Defaults to quarterly.

        Returns:
            pd.DataFrame: A DataFrame with columns ['period', 'merged_prs', 'opened_issues'].
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        issues_df = self._get_data_as_df('issues', 'created_at')

        merged = pd.DataFrame()
        if not prs_df.empty and 'merged_at' in prs_df.columns and 'state' in prs_df.columns:
            merged = prs_df[prs_df['state'] == 'merged'].copy()

        if not merged.empty:
            merged['merged_at'] = pd.to_datetime(merged['merged_at'], utc=True)
            merged_counts = (
                merged.groupby(pd.Grouper(key='merged_at', freq=freq))
                .size()
                .reset_index(name='merged_prs')
                .rename(columns={'merged_at': 'period'})
            )
        else:
            merged_counts = pd.DataFrame(columns=['period', 'merged_prs'])

        if not issues_df.empty:
            issues_df = issues_df.copy()
            issues_df['created_at'] = pd.to_datetime(issues_df['created_at'], utc=True)
            opened_counts = (
                issues_df.groupby(pd.Grouper(key='created_at', freq=freq))
                .size()
                .reset_index(name='opened_issues')
                .rename(columns={'created_at': 'period'})
            )
        else:
            opened_counts = pd.DataFrame(columns=['period', 'opened_issues'])

        if merged_counts.empty and opened_counts.empty:
            return pd.DataFrame(columns=['period', 'merged_prs', 'opened_issues'])

        result = pd.merge(merged_counts, opened_counts, on='period', how='outer').fillna(0)
        result = result.sort_values('period').reset_index(drop=True)
        result['merged_prs'] = result['merged_prs'].astype(int)
        result['opened_issues'] = result['opened_issues'].astype(int)
        return result[['period', 'merged_prs', 'opened_issues']]

    def get_new_contributors_timeline(self, freq: str = 'QE') -> pd.DataFrame:
        """
        Get the count of new (first-time) contributors per period.

        A contributor is "new" in the period containing their first-ever
        contribution (PR, issue, comment, or review), computed over the
        full history of the repo regardless of self.days.

        Args:
            freq (str): The pandas frequency alias to bucket periods by
                        (e.g. 'QE' for quarter-end). Defaults to quarterly.

        Returns:
            pd.DataFrame: A DataFrame with columns ['period', 'new_contributors'].
        """
        first_contribution = self._first_contribution_dates()
        if first_contribution.empty:
            return pd.DataFrame(columns=['period', 'new_contributors'])

        df = first_contribution.reset_index(name='timestamp')
        result = (
            df.groupby(pd.Grouper(key='timestamp', freq=freq))
            .size()
            .reset_index(name='new_contributors')
            .rename(columns={'timestamp': 'period'})
        )
        return result[['period', 'new_contributors']]

    def get_cumulative_contributors(self, freq: str = 'QE') -> pd.DataFrame:
        """
        Get the cumulative count of distinct contributors over time (Goal 5).

        Built on _first_contribution_dates(), which spans the full history of
        the repo regardless of self.days. Each contributor is counted in the
        period of their first-ever contribution; the running total shows how
        the contributor base has grown, so evaluations can ask whether
        new-contributor growth is accelerating.

        Args:
            freq (str): The pandas frequency alias to bucket periods by
                        (e.g. 'QE' for quarter-end). Defaults to quarterly.

        Returns:
            pd.DataFrame: A DataFrame with columns ['period',
                          'new_contributors', 'cumulative_contributors']. The
                          final cumulative value equals the repo's total number
                          of distinct contributors.
        """
        columns = ['period', 'new_contributors', 'cumulative_contributors']
        first_contribution = self._first_contribution_dates()
        if first_contribution.empty:
            return pd.DataFrame(columns=columns)

        df = first_contribution.reset_index(name='timestamp')
        result = (
            df.groupby(pd.Grouper(key='timestamp', freq=freq))
            .size()
            .reset_index(name='new_contributors')
            .rename(columns={'timestamp': 'period'})
        )
        result['cumulative_contributors'] = result['new_contributors'].cumsum()
        return result[columns]

    def get_contributor_summary(self) -> dict:
        """
        Get a summary of contributor activity in the current time window.

        Returns:
            dict: A dictionary containing:
                - total_contributors: Number of unique logins active in the window.
                - new_contributors: Number of contributors whose first-ever
                                    contribution falls inside the window (for
                                    an all-time window this equals total_contributors).
                - total_contributions: Total PRs + issues + comments + reviews in the window.
        """
        prs_df = self._get_data_as_df('pull_requests', 'created_at')
        issues_df = self._get_data_as_df('issues', 'created_at')
        comments_df = self._get_data_as_df('comments', 'created_at')
        reviews_df = self._get_data_as_df('reviews', 'submitted_at')

        window_logins = set()
        if not prs_df.empty:
            window_logins |= set(prs_df['user_login'].unique())
        if not issues_df.empty:
            window_logins |= set(issues_df['user_login'].unique())
        if not comments_df.empty:
            window_logins |= set(comments_df['user_login'].unique())
        if not reviews_df.empty:
            window_logins |= set(reviews_df['reviewer_login'].unique())

        total_contributions = len(prs_df) + len(issues_df) + len(comments_df) + len(reviews_df)

        if self.days is None:
            # The window is all time, so every contributor is "new" within it.
            new_contributors = len(window_logins)
        else:
            first_contribution = self._first_contribution_dates()
            new_contributors = sum(
                1 for login in window_logins
                if login in first_contribution.index and first_contribution[login] >= self.start_date
            )

        return {
            "total_contributors": len(window_logins),
            "new_contributors": new_contributors,
            "total_contributions": total_contributions
        }

    # commit-based contributor metrics (Goal 5, alternate definition)
    #
    # These count contributors the way GitHub's own "Contributors" graph does:
    # by authorship of non-merge commits on the default branch, keyed on the
    # linked GitHub account (author_login). This is deliberately narrower than
    # the engagement-based contributor count (PR/issue/comment/review authors)
    # -- someone who opens an issue but never lands a commit is an engagement
    # contributor but not a commit contributor. Both definitions are reported
    # side by side so readers always know which one they are looking at.
    # -- helpers --

    def _first_commit_dates(self) -> pd.Series:
        """
        Get each commit author's first-ever non-merge commit timestamp.

        Considers only non-merge commits with a non-null author_login (an
        unlinked commit email cannot be attributed to a distinct GitHub
        account) and looks across the full history of the repo (ignoring
        self.days), mirroring _first_contribution_dates so a narrow window does
        not make established committers look "new".

        Returns:
            pd.Series: Series indexed by login, with the earliest non-merge
                       commit timestamp (UTC) as the value. Empty if there is
                       no qualifying commit data.
        """
        commits_df = self._get_data_as_df('commits', 'authored_at',
                                          ignore_time_filter=True)
        if commits_df.empty:
            return pd.Series(dtype='datetime64[ns, UTC]')

        non_merge = commits_df[~commits_df['is_merge_commit'].fillna(False).astype(bool)]
        non_merge = non_merge[non_merge['author_login'].notna()]
        if non_merge.empty:
            return pd.Series(dtype='datetime64[ns, UTC]')

        non_merge = non_merge.copy()
        non_merge['authored_at'] = pd.to_datetime(non_merge['authored_at'], utc=True)
        return non_merge.groupby('author_login')['authored_at'].min()

    # -- valueboxes / charts --

    def get_commit_contributor_summary(self) -> dict:
        """
        Get a commit-based summary of contributor activity in the current window.

        This is the GitHub-contributor-graph definition of "contributor":
        distinct GitHub accounts (author_login) that authored a non-merge
        commit inside the window. Merge commits and commits whose email is not
        linked to a GitHub account (author_login IS NULL) are excluded from the
        counts; bot authors are excluded via the central bot filter. This
        matches GitHub's own contributor graph to within ~1%.

        Returns:
            dict: A dictionary containing:
                - commit_contributors: Number of distinct non-null author_login
                                       values on non-merge commits in the window.
                - total_commits: Number of non-merge commits in the window
                                 (including NULL-login commits, which are still
                                 real commits even though they cannot be
                                 attributed to a distinct contributor).
        """
        commits_df = self._get_data_as_df('commits', 'authored_at')
        if commits_df.empty:
            return {"commit_contributors": 0, "total_commits": 0}

        non_merge = commits_df[~commits_df['is_merge_commit'].fillna(False).astype(bool)]
        if non_merge.empty:
            return {"commit_contributors": 0, "total_commits": 0}

        contributors = non_merge['author_login'].dropna().nunique()
        return {
            "commit_contributors": int(contributors),
            "total_commits": int(len(non_merge)),
        }

    def get_commit_contributors_timeline(self, freq: str = 'QE') -> pd.DataFrame:
        """
        Get the count of new and cumulative commit contributors per period.

        A commit contributor is "new" in the period containing their first-ever
        non-merge commit (keyed on author_login), computed over the full
        history of the repo regardless of self.days -- mirroring
        get_cumulative_contributors but for the commit-based definition. The
        running total shows how the commit-contributor base has grown.

        Args:
            freq (str): The pandas frequency alias to bucket periods by
                        (e.g. 'QE' for quarter-end). Defaults to quarterly.

        Returns:
            pd.DataFrame: A DataFrame with columns ['period',
                          'new_commit_contributors',
                          'cumulative_commit_contributors']. The final
                          cumulative value equals the repo's total number of
                          distinct commit contributors.
        """
        columns = ['period', 'new_commit_contributors', 'cumulative_commit_contributors']
        first_commit = self._first_commit_dates()
        if first_commit.empty:
            return pd.DataFrame(columns=columns)

        df = first_commit.reset_index(name='timestamp')
        result = (
            df.groupby(pd.Grouper(key='timestamp', freq=freq))
            .size()
            .reset_index(name='new_commit_contributors')
            .rename(columns={'timestamp': 'period'})
        )
        result['cumulative_commit_contributors'] = result['new_commit_contributors'].cumsum()
        return result[columns]

    # discussion / support metrics (Goal 3: decrease the time for support
    # responses; broaden participation in the support forum)
    # -- valueboxes --

    def _discussion_first_response_times(self, ignore_time_filter: bool = False) -> pd.DataFrame:
        """
        Get, per discussion, the time to its first non-author response.

        A "response" is any top-level comment or reply authored by someone
        other than the discussion's own author (self-replies don't count as
        support). Discussions are restricted to the current time window
        (unless ignore_time_filter is True, used by trend series that need
        full history), but their comments are always looked up across all
        time, since a discussion opened inside the window may not be answered
        until after it. Bot authors are excluded via the central bot filter on
        both tables. Discussions with no qualifying response are omitted.

        Args:
            ignore_time_filter (bool): If True, include discussions from all
                                       history rather than only the current
                                       self.days window.

        Returns:
            pd.DataFrame: A DataFrame with columns ['created_at',
                          'response_days'] — the discussion's creation
                          timestamp (UTC) and the days to its first response.
                          Empty (with those columns) if there is no data.
        """
        columns = ['created_at', 'response_days']
        discussions_df = self._get_data_as_df(
            'discussions', 'created_at', ignore_time_filter=ignore_time_filter)
        if discussions_df.empty:
            return pd.DataFrame(columns=columns)

        comments_df = self._get_data_as_df(
            'discussion_comments', 'created_at', ignore_time_filter=True)
        if comments_df.empty:
            return pd.DataFrame(columns=columns)
        comments_df = comments_df.copy()
        comments_df['created_at'] = pd.to_datetime(comments_df['created_at'], utc=True)

        records = []
        for _, discussion in discussions_df.iterrows():
            created = pd.to_datetime(discussion['created_at'], utc=True)
            thread_comments = comments_df[
                (comments_df['discussion_number'] == discussion['number']) &
                (comments_df['author_login'] != discussion['author_login'])
            ]
            if thread_comments.empty:
                continue
            first_response = thread_comments['created_at'].min()
            records.append({
                'created_at': created,
                'response_days': (first_response - created).total_seconds() / 86400,
            })

        if not records:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(records)

    def get_med_time_to_first_discussion_response(self) -> float:
        """
        Get the median time to the first response on a discussion.

        A "response" is any top-level comment or reply authored by someone
        other than the discussion's own author (self-replies don't count as
        support). Discussions are restricted to the current time window, but
        their comments are looked up across all time, since a discussion
        opened inside the window may not be answered until after it. Bot
        authors are excluded via the central bot filter on both tables.
        Discussions with no qualifying response are excluded from the median.

        Returns:
            float: The median time to first response in days, or 0.0 if
                   there is no data.
        """
        times = self._discussion_first_response_times()
        if times.empty:
            return 0.0
        return float(times['response_days'].median())

    def get_med_time_to_answer(self) -> float:
        """
        Get the median time to answer for answered discussions.

        Returns:
            float: The median of (answer_created_at - created_at) in days,
                   over discussions marked as answered in the current time
                   window. Returns 0.0 if there is no data.
        """
        discussions_df = self._get_data_as_df('discussions', 'created_at')
        if discussions_df.empty or 'answer_created_at' not in discussions_df.columns:
            return 0.0

        answered = discussions_df[
            discussions_df['is_answered'].fillna(False).astype(bool) &
            discussions_df['answer_created_at'].notna()
        ].copy()
        if answered.empty:
            return 0.0

        answered['created_at'] = pd.to_datetime(answered['created_at'], utc=True)
        answered['answer_created_at'] = pd.to_datetime(answered['answer_created_at'], utc=True)
        answered['time_to_answer'] = (
            answered['answer_created_at'] - answered['created_at']
        ).dt.total_seconds() / 86400
        return float(answered['time_to_answer'].median())

    def get_discussion_answer_rate(self) -> dict:
        """
        Get the discussion answer rate in the given time period.

        The headline rate is computed across all discussion categories for
        consistency with other "answer/close rate" metrics. A second rate is
        also computed restricted to the Q&A category, since Q&A is the clear
        support signal (General/Ideas discussions often legitimately go
        unanswered).

        Returns:
            dict: A dictionary with:
                - answer_rate: Percentage of all discussions answered.
                - total_answered: Count of answered discussions (all categories).
                - total_discussions: Count of all discussions.
                - qa_answer_rate: Percentage of Q&A discussions answered.
                - qa_answered: Count of answered Q&A discussions.
                - qa_total: Count of Q&A discussions.
        """
        empty_result = {
            "answer_rate": 0.0,
            "total_answered": 0,
            "total_discussions": 0,
            "qa_answer_rate": 0.0,
            "qa_answered": 0,
            "qa_total": 0,
        }
        discussions_df = self._get_data_as_df('discussions', 'created_at')
        if discussions_df.empty:
            return empty_result

        # is_answered is NULL (not False) for categories other than Q&A,
        # since GitHub only supports marking an accepted answer there;
        # fillna(False) avoids treating those NULLs as answered (NaN is
        # truthy under a bare astype(bool)).
        is_answered = discussions_df['is_answered'].fillna(False).astype(bool)
        total_discussions = len(discussions_df)
        total_answered = int(is_answered.sum())
        answer_rate = 100.0 * total_answered / total_discussions if total_discussions > 0 else 0.0

        qa_df = discussions_df[discussions_df.get('category') == 'Q&A']
        qa_total = len(qa_df)
        qa_answered = int(qa_df['is_answered'].fillna(False).astype(bool).sum()) if qa_total > 0 else 0
        qa_answer_rate = 100.0 * qa_answered / qa_total if qa_total > 0 else 0.0

        return {
            "answer_rate": answer_rate,
            "total_answered": total_answered,
            "total_discussions": total_discussions,
            "qa_answer_rate": qa_answer_rate,
            "qa_answered": qa_answered,
            "qa_total": qa_total,
        }

    # -- charts --

    def get_responder_diversity(self, freq: str = 'QE') -> pd.DataFrame:
        """
        Get the count of unique discussion responders per period, split into
        core team vs. community.

        This is a Goal 3 sustainability metric: broadening participation in
        the support forum means more of the responses should come from the
        community rather than just the core team.

        Args:
            freq (str): The pandas frequency alias to bucket periods by
                        (e.g. 'QE' for quarter-end). Defaults to quarterly.

        Returns:
            pd.DataFrame: A DataFrame with columns ['period', 'unique_responders',
                          'core_team_responders', 'community_responders'].
        """
        columns = ['period', 'unique_responders', 'core_team_responders', 'community_responders']
        comments_df = self._get_data_as_df('discussion_comments', 'created_at')
        if comments_df.empty:
            return pd.DataFrame(columns=columns)

        comments_df = comments_df.copy()
        comments_df['created_at'] = pd.to_datetime(comments_df['created_at'], utc=True)
        core_logins = team.core_team_logins()

        rows = []
        for period, group in comments_df.groupby(pd.Grouper(key='created_at', freq=freq)):
            authors = set(group['author_login'].unique())
            rows.append({
                'period': period,
                'unique_responders': len(authors),
                'core_team_responders': len(authors & core_logins),
                'community_responders': len(authors - core_logins),
            })

        return pd.DataFrame(rows, columns=columns)

    def get_discussions_opened_by_category(self, freq: str = 'QE') -> pd.DataFrame:
        """
        Get the count of discussions opened per period, broken down by category.

        Returns:
            pd.DataFrame: A DataFrame with columns ['period', 'category', 'count'].
        """
        columns = ['period', 'category', 'count']
        discussions_df = self._get_data_as_df('discussions', 'created_at')
        if discussions_df.empty:
            return pd.DataFrame(columns=columns)

        discussions_df = discussions_df.copy()
        discussions_df['created_at'] = pd.to_datetime(discussions_df['created_at'], utc=True)
        discussions_df['category'] = discussions_df['category'].fillna('Uncategorized')

        result = (
            discussions_df.groupby([pd.Grouper(key='created_at', freq=freq), 'category'])
            .size()
            .reset_index(name='count')
            .rename(columns={'created_at': 'period'})
        )
        return result[columns]

    def get_response_share(self) -> dict:
        """
        Get the share of discussion responses authored by the core team vs.
        the community in the current time window.

        This is a Goal 3 sustainability metric: broadening participation in
        the support forum means the community should account for a growing
        share of responses, not just the core team.

        Returns:
            dict: A dictionary with:
                - core_team_pct: Percentage of responses by core team members.
                - community_pct: Percentage of responses by everyone else.
                - total_responses: Total discussion comments in the window.
        """
        comments_df = self._get_data_as_df('discussion_comments', 'created_at')
        if comments_df.empty:
            return {"core_team_pct": 0.0, "community_pct": 0.0, "total_responses": 0}

        core_logins = team.core_team_logins()
        total_responses = len(comments_df)
        core_count = int(comments_df['author_login'].isin(core_logins).sum())
        community_count = total_responses - core_count

        return {
            "core_team_pct": 100.0 * core_count / total_responses,
            "community_pct": 100.0 * community_count / total_responses,
            "total_responses": total_responses,
        }

    def get_discussion_response_trend(self, freq: str = 'QE') -> pd.DataFrame:
        """
        Get the median time to first discussion response per period (Goal 3 trend).

        Trends span the full history of the repo regardless of self.days.
        Discussions are bucketed by their creation date and mapped to their
        time to first non-author response (reusing the pairing logic of
        get_med_time_to_first_discussion_response); the median is taken per
        period. Discussions with no qualifying response are excluded.

        Args:
            freq (str): The pandas frequency alias to bucket periods by
                        (e.g. 'QE' for quarter-end). Defaults to quarterly.

        Returns:
            pd.DataFrame: A DataFrame with columns ['period', 'median_days'].
        """
        columns = ['period', 'median_days']
        times = self._discussion_first_response_times(ignore_time_filter=True)
        if times.empty:
            return pd.DataFrame(columns=columns)

        result = (
            times.groupby(pd.Grouper(key='created_at', freq=freq))['response_days']
            .median()
            .reset_index(name='median_days')
            .rename(columns={'created_at': 'period'})
        )
        result = result.dropna(subset=['median_days'])
        return result[columns]


