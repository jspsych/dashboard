"""
Database manager module
Handles SQLite database operations, CRUD operations, and data management
"""

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

from . import repos
from .models import DatabaseHelper, DatabaseSchema

# Tables that carry a per-repo `repo` column and are subject to auto-migration.
_REPO_TABLES = ('pull_requests', 'issues', 'reviews', 'comments', 'releases')


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
        """Create all tables and indexes if they don't exist.

        Runs an idempotent auto-migration first: any pre-existing table that
        lacks the `repo` column is rebuilt into the repo-aware schema (existing
        rows are attributed to the main repo). Fresh databases simply get the
        new schema from the CREATE TABLE statements.
        """
        migrated = self._migrate_to_repo_schema()
        if migrated:
            self.logger.info(f"Migrated tables to repo-aware schema: {migrated}")
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

    def _migrate_to_repo_schema(self) -> List[str]:
        """Rebuild any legacy table missing the `repo` column, atomically.

        For each affected table the migration, inside a single transaction:
          1. creates a new-schema table,
          2. copies every existing row across, stamping repo = main repo,
          3. drops the old table and renames the new one into place.
        Indexes (including the new per-repo indexes) are recreated afterwards by
        initialize_database. Returns the list of tables that were migrated.
        """
        conn = sqlite3.connect(self.db_path)
        conn.isolation_level = None  # manual transaction control
        migrated: List[str] = []
        try:
            conn.execute("PRAGMA foreign_keys=OFF")
            existing = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            create_stmts = DatabaseSchema.get_create_table_statements()
            conn.execute("BEGIN")
            for table in _REPO_TABLES:
                if table not in existing:
                    continue
                cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
                if 'repo' in cols:
                    continue  # already migrated
                new_create = create_stmts[table].replace(
                    f"IF NOT EXISTS {table}", f"{table}_new"
                )
                col_list = ", ".join(cols)
                conn.execute(f"DROP TABLE IF EXISTS {table}_new")
                conn.execute(new_create)
                conn.execute(
                    f"INSERT INTO {table}_new ({col_list}, repo) "
                    f"SELECT {col_list}, ? FROM {table}",
                    (repos.MAIN_REPO,),
                )
                conn.execute(f"DROP TABLE {table}")
                conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")
                migrated.append(table)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            self.logger.error("Schema migration failed; rolled back", exc_info=True)
            raise
        finally:
            conn.close()
        return migrated
    
    def _initialize_metadata(self, conn: sqlite3.Connection):
        """Initialize metadata table with default values"""
        now = datetime.utcnow().isoformat()
        default_metadata = [
            ('last_pr_sync', '2020-01-01T00:00:00'),
            ('last_issue_sync', '2020-01-01T00:00:00'),
            ('last_full_sync', '2020-01-01T00:00:00'),
            ('last_incremental_sync', '2020-01-01T00:00:00'),
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
                        id, repo, number, title, body, state, created_at, updated_at,
                        closed_at, merged_at, user_login, user_type, base_branch,
                        head_branch, additions, deletions, changed_files, commits_count,
                        labels, assignees, draft, mergeable, is_breaking_change,
                        pr_type, first_response_at, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    pr_data['id'], pr_data['repo'], pr_data['number'], pr_data['title'], pr_data.get('body'),
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
    
    def get_pull_requests(self, repo: Optional[str] = None, state: Optional[str] = None,
                          limit: Optional[int] = None) -> List[Dict]:
        """Get pull requests with optional filtering by repo and/or state"""
        with self.get_connection() as conn:
            query = 'SELECT * FROM pull_requests'
            params = []
            conditions = []
            if repo:
                conditions.append('repo = ?')
                params.append(repo)
            if state:
                conditions.append('state = ?')
                params.append(state)
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)

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
                        id, repo, number, title, body, state, created_at, updated_at,
                        closed_at, user_login, user_type, assignee_login, labels,
                        comments_count, issue_type, priority, first_response_at,
                        is_external_user, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    issue_data['id'], issue_data['repo'], issue_data['number'], issue_data['title'],
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
    
    def get_issues(self, repo: Optional[str] = None, state: Optional[str] = None,
                   limit: Optional[int] = None) -> List[Dict]:
        """Get issues with optional filtering by repo and/or state"""
        with self.get_connection() as conn:
            query = 'SELECT * FROM issues'
            params = []
            conditions = []
            if repo:
                conditions.append('repo = ?')
                params.append(repo)
            if state:
                conditions.append('state = ?')
                params.append(state)
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)

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
        
    def get_pull_request_by_number(self, repo: str, number: int) -> Optional[Dict[str, Any]]:
        """Get a single pull request by its repo and number."""
        with self.get_connection() as conn:
            row = conn.execute(
                'SELECT * FROM pull_requests WHERE repo = ? AND number = ?',
                (repo, number),
            ).fetchone()
            if row:
                pr = dict(row)
                pr['labels'] = DatabaseHelper.deserialize_labels(pr['labels'])
                pr['assignees'] = DatabaseHelper.deserialize_assignees(pr['assignees'])
                return pr
            return None
    
    def upsert_review(self, review_data: Dict[str, Any]) -> bool:
        """Insert or update a review record"""
        try:
            with self.get_connection() as conn:
                now = datetime.utcnow().isoformat()
                
                conn.execute('''
                    INSERT OR REPLACE INTO reviews (
                        id, repo, pr_number, reviewer_login, state, submitted_at,
                        body, commit_sha, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    review_data['id'], review_data['repo'], review_data['pr_number'],
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
                        id, repo, issue_number, pr_number, user_login, body, created_at,
                        updated_at, comment_type, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    comment_data['id'], comment_data['repo'], comment_data.get('issue_number'),
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
                        id, repo, tag_name, name, body, created_at, published_at, draft,
                        prerelease, author_login, tarball_url, zipball_url,
                        is_breaking, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    release_data['id'], release_data['repo'], release_data['tag_name'], release_data.get('name'),
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

    def upsert_discussion(self, data: Dict[str, Any]) -> bool:
        """Insert or update a discussion record"""
        try:
            with self.get_connection() as conn:
                now = datetime.utcnow().isoformat()
                conn.execute('''
                    INSERT OR REPLACE INTO discussions (
                        id, repo, number, title, body, category, created_at,
                        updated_at, author_login, is_answered, answer_comment_id,
                        answer_created_at, upvote_count, comments_count, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['id'], data['repo'], data['number'], data['title'],
                    data.get('body'), data.get('category'), data['created_at'],
                    data['updated_at'], data['author_login'],
                    data.get('is_answered', False), data.get('answer_comment_id'),
                    data.get('answer_created_at'), data.get('upvote_count'),
                    data.get('comments_count'), now
                ))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Error upserting discussion {data.get('number', 'unknown')}: {e}")
            return False

    def upsert_discussion_comment(self, data: Dict[str, Any]) -> bool:
        """Insert or update a discussion comment (or reply) record"""
        try:
            with self.get_connection() as conn:
                now = datetime.utcnow().isoformat()
                conn.execute('''
                    INSERT OR REPLACE INTO discussion_comments (
                        id, repo, discussion_number, author_login, body,
                        created_at, is_answer, parent_comment_id, last_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['id'], data['repo'], data['discussion_number'],
                    data['author_login'], data.get('body'), data['created_at'],
                    data.get('is_answer', False), data.get('parent_comment_id'), now
                ))
                conn.commit()
                return True
        except Exception as e:
            self.logger.error(f"Error upserting discussion comment {data.get('id', 'unknown')}: {e}")
            return False

    def get_discussions(self, repo: Optional[str] = None,
                        limit: Optional[int] = None) -> List[Dict]:
        """Get discussions with optional filtering by repo"""
        with self.get_connection() as conn:
            query = 'SELECT * FROM discussions'
            params: List[Any] = []
            if repo:
                query += ' WHERE repo = ?'
                params.append(repo)
            query += ' ORDER BY created_at DESC'
            if limit:
                query += ' LIMIT ?'
                params.append(limit)
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

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
    
    def get_last_sync_time(self, repo: Optional[str] = None) -> Optional[str]:
        """Return the most recent sync timestamp for display.

        Sync metadata is stored per repo under keys like
        `last_pr_sync:jspsych/jsPsych`. With no repo argument this returns the
        most recent sync across all repos (keeping the dashboard's single
        "Last Sync Time" display working); with a repo it scopes to that repo.
        """
        with self.get_connection() as conn:
            if repo:
                row = conn.execute(
                    "SELECT MAX(updated_at) AS ts FROM metadata "
                    "WHERE key LIKE 'last_%_sync:' || ?",
                    (repo,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT MAX(updated_at) AS ts FROM metadata "
                    "WHERE key LIKE 'last_%_sync%'"
                ).fetchone()
            return row['ts'] if row and row['ts'] else None

    def get_sync_time_value(self, sync_type: str, repo: str) -> Optional[str]:
        """Return the stored ISO timestamp value of a per-repo sync marker."""
        return self.get_metadata(f'last_{sync_type}_sync:{repo}')

    def update_last_sync_time(self, sync_type: str, repo: str):
        """Update last sync timestamp for a specific sync type and repo"""
        now = datetime.utcnow().isoformat()
        self.set_metadata(f'last_{sync_type}_sync:{repo}', now)
    
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
            '''.format(days, days)
            
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
