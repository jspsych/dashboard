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
}


class MetricsCalculator:
    """
    A class to calculate various repository metrics from a database.
    
    Attributes:
        db (DatabaseManager): An instance of the database manager.
        days (Optional[int]): The number of days to look back for metrics. If None, calculates for all time.
        end_date (datetime): The end date for the time window (current time).
        start_date (Optional[datetime]): The start date for the time window.
    """
    def __init__(self, db_path: str = "../data/analytics.db", days: Optional[int] = 30,
                 repo: Optional[str] = repos.MAIN_REPO):
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
        """
        self.db = DatabaseManager(db_path)
        self.days = days
        self.repo = repo
        self.end_date = datetime.now(timezone.utc)
        
        if self.days is not None:
            self.start_date = self.end_date - timedelta(days=self.days)
        else:
            self.start_date = None # For "all time" calculations

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
            # user_type is only present on pull_requests and issues.
            if 'user_type' in df.columns:
                is_bot_mask = df.apply(
                    lambda row: team.is_bot(row[author_column], row['user_type']),
                    axis=1
                )
            else:
                is_bot_mask = df[author_column].apply(team.is_bot)
            df = df[~is_bot_mask].copy()
            if df.empty:
                return pd.DataFrame()

        if self.start_date and date_column in df.columns and not ignore_time_filter:
            df[date_column] = pd.to_datetime(df[date_column], utc=True)
            return df[df[date_column] >= self.start_date].copy()

        return df

    # overview metrics
    # -- valueboxes --
    def get_active_items(self) -> Dict[str, int]:
        """
        Get the count of active pull requests and issues.

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


