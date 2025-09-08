"""
Calculates repository metrics based on data in the SQLite database.

This module provides a MetricsCalculator class that can be instantiated for
different time periods (e.g., 30, 60, 90 days, or all time) to compute
a wide range of metrics related to pull requests, issues, and community
engagement.
"""

import sys
import os


import pandas as pd
import numpy as np
from .database import DatabaseManager
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List


class MetricsCalculator:
    """
    A class to calculate various repository metrics from a database.
    
    Attributes:
        db (DatabaseManager): An instance of the database manager.
        days (Optional[int]): The number of days to look back for metrics. If None, calculates for all time.
        end_date (datetime): The end date for the time window (current time).
        start_date (Optional[datetime]): The start date for the time window.
    """
    def __init__(self, db_path: str = "../data/analytics.db", days: Optional[int] = 30):
        """
        Initializes the MetricsCalculator.

        Args:
            db_path (str): The path to the SQLite database file.
            days (Optional[int]): The number of days for the metrics window.
                                  Pass None for "all time" metrics.
        """
        self.db = DatabaseManager(db_path)
        self.days = days
        self.end_date = datetime.now(timezone.utc)
        
        if self.days is not None:
            self.start_date = self.end_date - timedelta(days=self.days)
        else:
            self.start_date = None # For "all time" calculations

    def _get_data_as_df(self, table_name: str, date_column: str = 'created_at') -> pd.DataFrame:
        """
        Fetch data from a table within the time window and return as a pandas DataFrame.
        
        Args:
            table_name (str): The name of the database table.
            date_column (str): The name of the date column to filter on.

        Returns:
            pd.DataFrame: A DataFrame containing the filtered data.
        """
        with self.db.get_connection() as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return pd.DataFrame()

        # Filter by date if a time window is specified
        if self.start_date and date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], utc=True)
            return df[df[date_column] >= self.start_date].copy()
        
        return df

    def get_total_pull_requests(self) -> int:
        """Calculates the total number of pull requests created in the period."""
        pr_df = self._get_data_as_df('pull_requests')
        return len(pr_df)

    def get_avg_reviews_per_pr(self) -> float:
        """Calculates the average number of reviews per pull request."""
        pr_df = self._get_data_as_df('pull_requests')
        if pr_df.empty:
            return 0.0
        
        reviews_df = self._get_data_as_df('reviews', date_column='submitted_at')
        if reviews_df.empty:
            return 0.0
            
        # Filter reviews for PRs in the current period
        reviews_for_prs = reviews_df[reviews_df['pr_number'].isin(pr_df['number'])]
        
        return len(reviews_for_prs) / len(pr_df) if not pr_df.empty else 0.0

    def get_time_to_first_review(self) -> float:
        """Calculates the average time from PR creation to first review in hours."""
        with self.db.get_connection() as conn:
            query = """
                SELECT 
                    p.created_at, 
                    MIN(r.submitted_at) as first_review_at
                FROM pull_requests p
                JOIN reviews r ON p.number = r.pr_number
            """
            if self.start_date:
                query += f" WHERE p.created_at >= '{self.start_date.isoformat()}'"
            query += " GROUP BY p.number"
            
            df = pd.read_sql_query(query, conn)

        if df.empty:
            return 0.0
        
        df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
        df['first_review_at'] = pd.to_datetime(df['first_review_at'], utc=True)
        df['time_to_review'] = (df['first_review_at'] - df['created_at']).dt.total_seconds() / 3600
        return df['time_to_review'].mean()

    def get_avg_lines_changed(self) -> float:
        """Calculates the average number of lines changed (additions + deletions) per PR."""
        df = self._get_data_as_df('pull_requests')
        if df.empty:
            return 0.0
        return (df['additions'].sum() + df['deletions'].sum()) / len(df)

    # --- Issue ValueBox Metrics ---
    def get_total_issues(self) -> int:
        """Calculates the total number of issues created in the period."""
        return len(self._get_data_as_df('issues'))

    def get_avg_issue_resolution_time(self) -> float:
        """Calculates the average time from issue creation to closing in days."""
        df = self._get_data_as_df('issues')
        closed_issues = df[df['state'] == 'closed'].copy()
        if closed_issues.empty or 'closed_at' not in closed_issues.columns:
            return 0.0
        
        closed_issues['created_at'] = pd.to_datetime(closed_issues['created_at'], utc=True)
        closed_issues['closed_at'] = pd.to_datetime(closed_issues['closed_at'], utc=True)
        
        # Drop rows where closed_at might be NaT
        closed_issues.dropna(subset=['closed_at'], inplace=True)
        if closed_issues.empty:
            return 0.0
            
        closed_issues['resolution_time'] = (closed_issues['closed_at'] - closed_issues['created_at']).dt.total_seconds() / (3600 * 24)
        return closed_issues['resolution_time'].mean()

    def get_time_to_first_response(self) -> float:
        """Calculates the average time from issue creation to first comment in hours."""
        with self.db.get_connection() as conn:
            query = """
                SELECT 
                    i.created_at, 
                    MIN(c.created_at) as first_comment_at
                FROM issues i
                JOIN comments c ON i.number = c.issue_number
                WHERE i.user_login != c.user_login
            """
            if self.start_date:
                query += f" AND i.created_at >= '{self.start_date.isoformat()}'"
            query += " GROUP BY i.number"
            
            df = pd.read_sql_query(query, conn)
            
        if df.empty:
            return 0.0
            
        df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
        df['first_comment_at'] = pd.to_datetime(df['first_comment_at'], utc=True)
        df['time_to_response'] = (df['first_comment_at'] - df['created_at']).dt.total_seconds() / 3600
        return df['time_to_response'].mean()

    def get_issue_resolution_rate(self) -> float:
        """Calculates the percentage of issues that were closed."""
        df = self._get_data_as_df('issues')
        if df.empty:
            return 0.0
        closed_count = df[df['state'] == 'closed'].shape[0]
        return (closed_count / len(df)) * 100

    # --- Overview ValueBox Metrics ---

    def get_merged_count(self) -> int:
        """Calculates the total number of merged pull requests."""
        df = self._get_data_as_df('pull_requests')
        if df.empty:
            return 0
        return df[df['state'] == 'merged'].shape[0]
    
    def get_merge_rate(self) -> float:
        """Calculates the percentage of PRs that were merged."""
        df = self._get_data_as_df('pull_requests')
        if df.empty:
            return 0.0
        merged_count = self.get_merged_count()
        return (merged_count / len(df)) * 100

    def get_active_items(self) -> Dict[str, int]:
        """Counts currently open PRs and issues."""
        with self.db.get_connection() as conn:
            open_prs = pd.read_sql_query("SELECT COUNT(*) as count FROM pull_requests WHERE state = 'open'", conn)['count'][0]
            open_issues = pd.read_sql_query("SELECT COUNT(*) as count FROM issues WHERE state = 'open'", conn)['count'][0]
        
        return {
            'open_prs': open_prs,
            'open_issues': open_issues,
            'total': open_prs + open_issues
        }

    def get_community_engagement(self) -> int:
        """Counts unique contributors (authors of PRs and issues)."""
        pr_df = self._get_data_as_df('pull_requests')
        issue_df = self._get_data_as_df('issues')
        
        pr_authors = set(pr_df['user_login']) if not pr_df.empty else set()
        issue_authors = set(issue_df['user_login']) if not issue_df.empty else set()
        
        return len(pr_authors.union(issue_authors))

    # --- Chart Data Functions ---
    def get_merge_time_distribution(self) -> Dict[str, int]:
        """Provides data for the PR merge time distribution chart."""
        df = self._get_data_as_df('pull_requests')
        merged_prs = df[df['merged_at'].notna()].copy()
        if merged_prs.empty:
            return {}
            
        merged_prs['created_at'] = pd.to_datetime(merged_prs['created_at'], utc=True)
        merged_prs['merged_at'] = pd.to_datetime(merged_prs['merged_at'], utc=True)
        merged_prs['merge_time_hours'] = (merged_prs['merged_at'] - merged_prs['created_at']).dt.total_seconds() / 3600
        
        bins = [0, 8, 24, 72, 168, np.inf] # <8h, 8-24h, 1-3d, 3-7d, >7d
        labels = ['< 8 Hours', '8-24 Hours', '1-3 Days', '3-7 Days', '> 7 Days']
        
        dist = pd.cut(merged_prs['merge_time_hours'], bins=bins, labels=labels, right=False).value_counts().sort_index()
        return dist.to_dict()

    def get_pr_size_distribution(self) -> Dict[str, int]:
        """Provides data for the PR size distribution chart based on lines changed."""
        df = self._get_data_as_df('pull_requests')
        if df.empty:
            return {}
            
        df['total_changes'] = df['additions'] + df['deletions']
        
        bins = [0, 50, 250, 1000, np.inf]
        labels = ['XS (<50)', 'Small (50-250)', 'Medium (250-1000)', 'Large (>1000)']
        
        dist = pd.cut(df['total_changes'], bins=bins, labels=labels, right=False).value_counts().sort_index()
        return dist.to_dict()
    
    def get_code_churn_trends(self) -> pd.DataFrame:
        """Provides data for code churn (additions/deletions) over time."""
        df = self._get_data_as_df('pull_requests')
        if df.empty:
            return pd.DataFrame({'date': [], 'additions': [], 'deletions': []})
        
        df['date'] = pd.to_datetime(df['created_at']).dt.to_period('W').dt.start_time
        churn = df.groupby('date')[['additions', 'deletions']].sum().reset_index()
        return churn

    def get_work_type_distribution(self) -> Dict[str, int]:
        """Aggregates PRs and issues by their classified type."""
        prs = self._get_data_as_df('pull_requests')
        issues = self._get_data_as_df('issues')
        
        pr_counts = prs['pr_type'].value_counts()
        issue_counts = issues['issue_type'].value_counts()
        
        combined = pr_counts.add(issue_counts, fill_value=0)
        return combined.to_dict()

    def get_open_issues_aging(self) -> Dict[str, int]:
        """Provides data for open issues aging distribution."""
        with self.db.get_connection() as conn:
            open_issues = pd.read_sql_query("SELECT created_at FROM issues WHERE state = 'open'", conn)

        if open_issues.empty:
            return {}

        open_issues['created_at'] = pd.to_datetime(open_issues['created_at'], utc=True)
        open_issues['age_days'] = (self.end_date - open_issues['created_at']).dt.days
        
        bins = [0, 30, 90, 365, np.inf]
        labels = ['< 30 Days', '30-90 Days', '90-365 Days', '> 1 Year']
        
        dist = pd.cut(open_issues['age_days'], bins=bins, labels=labels, right=False).value_counts().sort_index()
        return dist.to_dict()

    def get_backlog_health_trend(self) -> pd.DataFrame:
        """Provides data on created vs. closed issues over time."""
        df = self._get_data_as_df('issues', date_column='created_at')
        if df.empty:
            return pd.DataFrame()
        
        df['created_date'] = pd.to_datetime(df['created_at']).dt.to_period('W').dt.start_time
        df['closed_date'] = pd.to_datetime(df['closed_at']).dt.to_period('W').dt.start_time
        
        created = df.groupby('created_date').size().reset_index(name='created_count')
        closed = df.dropna(subset=['closed_date']).groupby('closed_date').size().reset_index(name='closed_count')
        
        trend = pd.merge(created, closed, left_on='created_date', right_on='closed_date', how='outer').fillna(0)
        trend['date'] = trend['created_date'].fillna(trend['closed_date'])
        return trend[['date', 'created_count', 'closed_count']].sort_values('date')

    def get_release_timeline(self) -> pd.DataFrame:
        """Gets release data for the release timeline chart."""
        return self._get_data_as_df('releases', date_column='published_at')


# if __name__ == '__main__':
#     # Example usage for different time windows:
#     # calculator_30d = MetricsCalculator(db_path="../data/analytics.db", days=30)
#     # calculator_60d = MetricsCalculator(db_path="../data/analytics.db", days=60)
#     # calculator_90d = MetricsCalculator(db_path="../data/analytics.db", days=90)
#     calculator_all = MetricsCalculator(db_path="../data/analytics.db", days=None) # All time

#     print("--- All-Time Metrics ---")
#     print(f"Total PRs: {calculator_all.get_total_pull_requests()}")
#     print(f"Total Issues: {calculator_all.get_total_issues()}")
#     print(f"Merge Rate: {calculator_all.get_merge_rate():.2f}%")
#     print(f"Issue Resolution Rate: {calculator_all.get_issue_resolution_rate():.2f}%")
#     print(f"Avg Time to First Review (hours): {calculator_all.get_time_to_first_review():.2f}")
#     print(f"Avg Issue Resolution Time (days): {calculator_all.get_avg_issue_resolution_time():.2f}")
    
#     active_items = calculator_all.get_active_items()
#     print(f"Active Items: {active_items['total']} ({active_items['open_issues']} issues, {active_items['open_prs']} PRs)")
    
#     print(f"Community Contributors: {calculator_all.get_community_engagement()}")

#     print("\n--- Chart Data Examples (All Time) ---")
#     print("Work Type Distribution:", calculator_all.get_work_type_distribution())
#     print("PR Size Distribution:", calculator_all.get_pr_size_distribution())
#     print("Open Issues Aging:", calculator_all.get_open_issues_aging())
