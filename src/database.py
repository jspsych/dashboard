"""
Database manager module
Handles SQLite database operations, CRUD operations, and data management
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import logging

from .models import DatabaseSchema, DatabaseHelper


class DatabaseManager:
    """Main database manager class for GitHub analytics"""
    def __init__(self, db_path: str = "data/analytics.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.initialize_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # return rows as dictionaries
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Database error: {e}")
            raise 
        finally:
            conn.close()
    
    def initialize_database(self):
        """Create all tables and indexes if they don't exist"""
        with self.get_connection() as conn:
            tables = DatabaseSchema.get_create_table_statements()
            for table_name, create_stmt in tables.items():
                conn.execute(create_stmt)
                self.logger.info(f"Created/verified table: {table_name}")
            indexes = DatabaseSchema.get_index_statements()
            for index_stmt in indexes:
                conn.execute(index_stmt)
            self._initialize_metadata(conn)
            conn.commit()
            self.logger.info("Database initialized successfully")
    
    def _initialize_metadata(self, conn: sqlite3.Connection):
        """Initialize metadata table with default values"""
        now = datetime.utcnow().isoformat()
        default_metadata = [
            ('last_pr_sync', '2020-01-01T00:00:00'),
            ('last_issue_sync', '2020-01-01T00:00:00'),
            ('last_full_sync', '2020-01-01T00:00:00'),
            ('total_prs_tracked', '0'),
            ('total_issues_tracked', '0'),
            ('database_version', '1.0'),
            ('created_at', now)
        ]
        for key, value in default_metadata:
            conn.execute(
                'INSERT OR IGNORE INTO metadata (key, value, updated_at) VALUES (?, ?, ?)',
                (key, value, now)
            )
        
    def upsert_pull_request(self, pr_data: Dict[str, Any]) -> bool:
        """Insert or update a pull request record"""
        try:
            with self.get_connection() as conn:
                labels = DatabaseHelper.serialize_labels(pr_data.get('labels', []))
                assignees = DatabaseHelper.serialize_assignees(pr_data.get('assignees', []))
                pr_type = DatabaseHelper.classify_pr_type(pr_data['title'], pr_data.get('labels', []))
                is_breaking = DatabaseHelper.is_breaking_change(
                    pr_data['title'], 
                    pr_data.get('body', ''), 
                    pr_data.get('labels', [])
                )
                now = datetime.utcnow().isoformat()
                conn.execute('''
                    INSERT OR REPLACE INTO pull_requests (
                        id, number, title, body, state, created_at, updated_at,
                        closed_at, merged_at, user_login, user_type, base_branch,
                        head_branch, additions, deletions, changed_files, commits_count,
                        labels, assignees, draft, mergeable, is_breaking_change,
                        pr_type, first_response_at, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    pr_data['id'], pr_data['number'], pr_data['title'], pr_data.get('body'),
                    pr_data['state'], pr_data['created_at'], pr_data['updated_at'],
                    pr_data.get('closed_at'), pr_data.get('merged_at'),
                    pr_data['user_login'], pr_data.get('user_type'),
                    pr_data['base_branch'], pr_data['head_branch'],
                    pr_data.get('additions'), pr_data.get('deletions'),
                    pr_data.get('changed_files'), pr_data.get('commits_count'),
                    labels, assignees, pr_data.get('draft', False),
                    pr_data.get('mergeable'), is_breaking, pr_type,
                    pr_data.get('first_response_at'), now
                ))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Error upserting PR {pr_data.get('number', 'unknown')}: {e}")
            return False
    
    def get_pull_requests(self, state: Optional[str] = None, limit: Optional[int] = None) -> List[Dict]:
        """Get pull requests with optional filtering"""
        with self.get_connection() as conn:
            query = 'SELECT * FROM pull_requests'
            params = []
            
            if state:
                query += ' WHERE state = ?'
                params.append(state)
            
            query += ' ORDER BY created_at DESC'
            
            if limit:
                query += ' LIMIT ?'
                params.append(limit)
            
            rows = conn.execute(query, params).fetchall()
            results = []
            for row in rows:
                pr = dict(row)
                pr['labels'] = DatabaseHelper.deserialize_labels(pr['labels'])
                pr['assignees'] = DatabaseHelper.deserialize_assignees(pr['assignees'])
                results.append(pr)
            
            return results
    
    def get_pr_metrics_summary(self, days: Optional[int] = None) -> Dict[str, Any]:
        """Get summary metrics for pull requests"""
        with self.get_connection() as conn:
            base_query = 'SELECT COUNT(*) as count, state FROM pull_requests'
            params = []
            if days:
                base_query += ' WHERE created_at > datetime("now", "-{} days")'.format(days)
            
            base_query += ' GROUP BY state'
            rows = conn.execute(base_query, params).fetchall()
            summary = {'total': 0, 'open': 0, 'closed': 0, 'merged': 0}
            for row in rows:
                summary[row['state']] = row['count']
                summary['total'] += row['count']
            return summary
        
    def upsert_issue(self, issue_data: Dict[str, Any]) -> bool:
        """Insert or update an issue record"""
        try:
            with self.get_connection() as conn:
                # Prepare data
                labels = DatabaseHelper.serialize_labels(issue_data.get('labels', []))
                issue_type = DatabaseHelper.classify_issue_type(issue_data['title'], issue_data.get('labels', []))
                priority = DatabaseHelper.get_priority_from_labels(issue_data.get('labels', []))
                
                now = datetime.utcnow().isoformat()
                
                conn.execute('''
                    INSERT OR REPLACE INTO issues (
                        id, number, title, body, state, created_at, updated_at,
                        closed_at, user_login, user_type, assignee_login, labels,
                        comments_count, issue_type, priority, first_response_at,
                        is_external_user, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    issue_data['id'], issue_data['number'], issue_data['title'],
                    issue_data.get('body'), issue_data['state'],
                    issue_data['created_at'], issue_data['updated_at'],
                    issue_data.get('closed_at'), issue_data['user_login'],
                    issue_data.get('user_type'), issue_data.get('assignee_login'),
                    labels, issue_data.get('comments_count', 0),
                    issue_type, priority, issue_data.get('first_response_at'),
                    issue_data.get('is_external_user', True), now
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Error upserting issue {issue_data.get('number', 'unknown')}: {e}")
            return False
    
    def get_issues(self, state: Optional[str] = None, limit: Optional[int] = None) -> List[Dict]:
        """Get issues with optional filtering"""
        with self.get_connection() as conn:
            query = 'SELECT * FROM issues'
            params = []
            
            if state:
                query += ' WHERE state = ?'
                params.append(state)
            
            query += ' ORDER BY created_at DESC'
            
            if limit:
                query += ' LIMIT ?'
                params.append(limit)
            
            rows = conn.execute(query, params).fetchall()
            results = []
            for row in rows:
                issue = dict(row)
                issue['labels'] = DatabaseHelper.deserialize_labels(issue['labels'])
                results.append(issue)
            
            return results
    
    def get_issue_metrics_summary(self, days: Optional[int] = None) -> Dict[str, Any]:
        with self.get_connection() as conn:
            base_query = 'SELECT COUNT(*) as count, state FROM issues'
            params = []
            
            if days:
                base_query += ' WHERE created_at > datetime("now", "-{} days")'.format(days)
            
            base_query += ' GROUP BY state'
            
            rows = conn.execute(base_query, params).fetchall()
            
            summary = {'total': 0, 'open': 0, 'closed': 0}
            for row in rows:
                summary[row['state']] = row['count']
                summary['total'] += row['count']
            
            return summary
    
    def upsert_review(self, review_data: Dict[str, Any]) -> bool:
        """Insert or update a review record"""
        try:
            with self.get_connection() as conn:
                now = datetime.utcnow().isoformat()
                
                conn.execute('''
                    INSERT OR REPLACE INTO reviews (
                        id, pr_number, reviewer_login, state, submitted_at,
                        body, commit_sha, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    review_data['id'], review_data['pr_number'],
                    review_data['reviewer_login'], review_data['state'],
                    review_data['submitted_at'], review_data.get('body'),
                    review_data.get('commit_sha'), now
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Error upserting review {review_data.get('id', 'unknown')}: {e}")
            return False

    def upsert_comment(self, comment_data: Dict[str, Any]) -> bool:
        """Insert or update a comment record"""
        try:
            with self.get_connection() as conn:
                now = datetime.utcnow().isoformat()
                conn.execute('''
                    INSERT OR REPLACE INTO comments (
                        id, issue_number, pr_number, user_login, body, created_at,
                        updated_at, comment_type, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    comment_data['id'], comment_data.get('issue_number'),
                    comment_data.get('pr_number'), comment_data['user_login'],
                    comment_data.get('body'), comment_data['created_at'],
                    comment_data['updated_at'], comment_data['comment_type'], now
                ))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Error upserting comment {comment_data.get('id', 'unknown')}: {e}")
            return False

    def upsert_release(self, release_data: Dict[str, Any]) -> bool:
        """Insert or update a release record"""
        try:
            with self.get_connection() as conn:
                now = datetime.utcnow().isoformat()
                conn.execute('''
                    INSERT OR REPLACE INTO releases (
                        id, tag_name, name, body, created_at, published_at, draft,
                        prerelease, author_login, tarball_url, zipball_url,
                        is_breaking, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    release_data['id'], release_data['tag_name'], release_data.get('name'),
                    release_data.get('body'), release_data['created_at'],
                    release_data.get('published_at'), release_data.get('draft', False),
                    release_data.get('prerelease', False), release_data.get('author_login'),
                    release_data.get('tarball_url'), release_data.get('zipball_url'),
                    release_data.get('is_breaking', False), now
                ))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Error upserting release {release_data.get('tag_name', 'unknown')}: {e}")
            return False

    def get_metadata(self, key: str) -> Optional[str]:
        """Get metadata value by key"""
        with self.get_connection() as conn:
            row = conn.execute('SELECT value FROM metadata WHERE key = ?', (key,)).fetchone()
            return row['value'] if row else None
    
    def set_metadata(self, key: str, value: str):
        """Set metadata key-value pair"""
        with self.get_connection() as conn:
            now = datetime.utcnow().isoformat()
            conn.execute('''
                INSERT OR REPLACE INTO metadata (key, value, updated_at)
                VALUES (?, ?, ?)
            ''', (key, value, now))
            conn.commit()
    
    def get_last_sync_time(self, sync_type: str) -> Optional[str]:
        """Get last sync timestamp for a specific sync type"""
        return self.get_metadata(f'last_{sync_type}_sync')
    
    def update_last_sync_time(self, sync_type: str):
        """Update last sync timestamp for a specific sync type"""
        now = datetime.utcnow().isoformat()
        self.set_metadata(f'last_{sync_type}_sync', now)
    
    def get_activity_timeline(self, days: int = 90) -> List[Dict[str, Any]]:
        """Get combined PR and issue activity over time"""
        with self.get_connection() as conn:
            query = '''
                SELECT 
                    DATE(created_at) as date,
                    'PR' as type,
                    COUNT(*) as count,
                    state
                FROM pull_requests 
                WHERE created_at > datetime("now", "-{} days")
                GROUP BY DATE(created_at), state
                
                UNION ALL
                
                SELECT 
                    DATE(created_at) as date,
                    'Issue' as type,
                    COUNT(*) as count,
                    state
                FROM issues 
                WHERE created_at > datetime("now", "-{} days")
                GROUP BY DATE(created_at), state
                
                ORDER BY date DESC
            '''.format(days)
            
            rows = conn.execute(query).fetchall()
            return [dict(row) for row in rows]
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get overall database statistics"""
        with self.get_connection() as conn:
            stats = {}
            tables = ['pull_requests', 'issues', 'reviews', 'comments', 'releases']
            for table in tables:
                count = conn.execute(f'SELECT COUNT(*) as count FROM {table}').fetchone()
                stats[f'{table}_count'] = count['count'] if count else 0

            pr_range = conn.execute('''
                SELECT MIN(created_at) as earliest, MAX(created_at) as latest 
                FROM pull_requests
            ''').fetchone()
            
            issue_range = conn.execute('''
                SELECT MIN(created_at) as earliest, MAX(created_at) as latest 
                FROM issues
            ''').fetchone()
            
            stats['pr_date_range'] = dict(pr_range) if pr_range and pr_range['earliest'] else None
            stats['issue_date_range'] = dict(issue_range) if issue_range and issue_range['earliest'] else None
            
            return stats
