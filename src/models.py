"""
Database models and schema definitions
"""

from datetime import datetime
from typing import Optional, List
import json
import re


class DatabaseSchema:
    """Database schema definitions and table creation"""

    @staticmethod
    def get_create_table_statements():
        """Returns all CREATE TABLE statements for the database"""
        tables = {
            'pull_requests': '''
                CREATE TABLE IF NOT EXISTS pull_requests (
                    id INTEGER PRIMARY KEY,
                    number INTEGER NOT NULL, -- PR number
                    title TEXT NOT NULL, -- PR title
                    body TEXT, -- PR description/body
                    state TEXT NOT NULL, -- 'open', 'closed', 'merged'
                    created_at TEXT NOT NULL, 
                    updated_at TEXT NOT NULL,
                    closed_at TEXT,
                    merged_at TEXT,
                    user_login TEXT NOT NULL, -- PR author login
                    user_type TEXT, -- 'User', 'Organization', etc. 
                    base_branch TEXT NOT NULL,
                    head_branch TEXT NOT NULL,
                    additions INTEGER DEFAULT 0,
                    deletions INTEGER DEFAULT 0,
                    changed_files INTEGER DEFAULT 0,
                    commits_count INTEGER DEFAULT 0,
                    labels TEXT, -- JSON array of label names
                    assignees TEXT, -- JSON array of assignee logins
                    draft BOOLEAN DEFAULT FALSE,
                    mergeable BOOLEAN,
                    is_breaking_change BOOLEAN DEFAULT FALSE,
                    pr_type TEXT, -- 'feature', 'bugfix', 'maintenance', 'docs'
                    first_response_at TEXT, -- when first review/comment was made
                    last_fetched_at TEXT NOT NULL,
                    UNIQUE(number)
                )
            ''',
            
            'issues': '''
                CREATE TABLE IF NOT EXISTS issues (
                    id INTEGER PRIMARY KEY,
                    number INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT,
                    state TEXT NOT NULL, -- 'open', 'closed'
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    closed_at TEXT,
                    user_login TEXT NOT NULL,
                    user_type TEXT,
                    assignee_login TEXT,
                    labels TEXT, -- JSON array of label names
                    comments_count INTEGER DEFAULT 0,
                    issue_type TEXT, -- 'bug', 'feature', 'question', 'documentation'
                    priority TEXT, -- 'low', 'medium', 'high', 'critical'
                    first_response_at TEXT, -- when first comment/assignment was made
                    is_external_user BOOLEAN DEFAULT TRUE, -- user is not a maintainer
                    last_fetched_at TEXT NOT NULL,
                    UNIQUE(number)
                )
            ''',
            
            'reviews': '''
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY,
                    pr_number INTEGER NOT NULL,
                    reviewer_login TEXT NOT NULL,
                    state TEXT NOT NULL, -- 'APPROVED', 'CHANGES_REQUESTED', 'COMMENTED', 'DISMISSED'
                    submitted_at TEXT NOT NULL,
                    body TEXT,
                    commit_sha TEXT,
                    last_fetched_at TEXT NOT NULL,
                    FOREIGN KEY (pr_number) REFERENCES pull_requests (number)
                )
            ''',
            
            'comments': '''
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY,
                    issue_number INTEGER, -- NULL if this is a PR comment
                    pr_number INTEGER, -- NULL if this is an issue comment
                    user_login TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    comment_type TEXT NOT NULL, -- 'issue', 'pr', 'review'
                    is_first_response BOOLEAN DEFAULT FALSE,
                    last_fetched_at TEXT NOT NULL,
                    CHECK ((issue_number IS NULL) != (pr_number IS NULL)) -- XOR constraint
                )
            ''',
            
            'releases': '''
                CREATE TABLE IF NOT EXISTS releases (
                    id INTEGER PRIMARY KEY,
                    tag_name TEXT NOT NULL,
                    name TEXT,
                    body TEXT,
                    created_at TEXT NOT NULL,
                    published_at TEXT,
                    draft BOOLEAN DEFAULT FALSE,
                    prerelease BOOLEAN DEFAULT FALSE,
                    author_login TEXT,
                    tarball_url TEXT,
                    zipball_url TEXT,
                    is_breaking BOOLEAN DEFAULT FALSE,
                    last_fetched_at TEXT NOT NULL,
                    UNIQUE(tag_name)
                )
            ''',

            'metadata': '''
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            '''
        }
        
        return tables
    
    @staticmethod
    def get_index_statements():
        """Returns CREATE INDEX statements for optimized queries"""
        
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_pr_state ON pull_requests (state)',
            'CREATE INDEX IF NOT EXISTS idx_pr_created_at ON pull_requests (created_at)',
            'CREATE INDEX IF NOT EXISTS idx_pr_merged_at ON pull_requests (merged_at)',
            'CREATE INDEX IF NOT EXISTS idx_pr_user ON pull_requests (user_login)',
            'CREATE INDEX IF NOT EXISTS idx_pr_type ON pull_requests (pr_type)',
            
            'CREATE INDEX IF NOT EXISTS idx_issue_state ON issues (state)',
            'CREATE INDEX IF NOT EXISTS idx_issue_created_at ON issues (created_at)',
            'CREATE INDEX IF NOT EXISTS idx_issue_closed_at ON issues (closed_at)',
            'CREATE INDEX IF NOT EXISTS idx_issue_user ON issues (user_login)',
            'CREATE INDEX IF NOT EXISTS idx_issue_type ON issues (issue_type)',
            
            'CREATE INDEX IF NOT EXISTS idx_review_pr_number ON reviews (pr_number)',
            'CREATE INDEX IF NOT EXISTS idx_review_reviewer ON reviews (reviewer_login)',
            'CREATE INDEX IF NOT EXISTS idx_review_submitted_at ON reviews (submitted_at)',
            
            'CREATE INDEX IF NOT EXISTS idx_comment_issue ON comments (issue_number)',
            'CREATE INDEX IF NOT EXISTS idx_comment_pr ON comments (pr_number)',
            'CREATE INDEX IF NOT EXISTS idx_comment_created_at ON comments (created_at)',
            
            'CREATE INDEX IF NOT EXISTS idx_release_published_at ON releases (published_at)',
            'CREATE INDEX IF NOT EXISTS idx_release_created_at ON releases (created_at)'
        ]
        
        return indexes


class DatabaseHelper:
    """Helper functions for database operations"""

    @staticmethod
    def serialize_labels(labels: List[str]) -> str:
        """Convert list of labels to JSON string"""
        return json.dumps(labels) if labels else '[]'
    
    @staticmethod
    def deserialize_labels(labels_json: str) -> List[str]:
        """Convert JSON string back to list of labels"""
        try:
            return json.loads(labels_json) if labels_json else []
        except json.JSONDecodeError:
            return []
    
    @staticmethod
    def serialize_assignees(assignees: List[str]) -> str:
        """Convert list of assignees to JSON string"""
        return json.dumps(assignees) if assignees else '[]'
    
    @staticmethod
    def deserialize_assignees(assignees_json: str) -> List[str]:
        """Convert JSON string back to list of assignees"""
        try:
            return json.loads(assignees_json) if assignees_json else []
        except json.JSONDecodeError:
            return []
        
    @staticmethod
    def format_timestamp(dt: Optional[datetime]) -> Optional[str]:
        """Format datetime to ISO string for database storage"""
        return dt.isoformat() if dt else None
    
    @staticmethod
    def parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
        """Parse ISO string back to datetime"""
        if not timestamp_str:
            return None
        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError:
            return None
    
    @staticmethod
    def classify_pr_type(title: str, labels: List[str]) -> str:
        """Classify PR type based on title and labels"""
        title_lower = title.lower()
        labels_lower = [label.lower() for label in labels]
        if any(label in labels_lower for label in ['bug', 'bugfix', 'fix']):
            return 'bugfix'
        elif any(label in labels_lower for label in ['feature', 'enhancement', 'new-feature']):
            return 'feature'
        elif any(label in labels_lower for label in ['documentation', 'docs']):
            return 'docs'
        elif any(label in labels_lower for label in ['maintenance', 'refactor', 'cleanup']):
            return 'maintenance'
        
        # Check title patterns
        if any(word in title_lower for word in ['fix', 'bug', 'patch', 'hotfix']):
            return 'bugfix'
        elif any(word in title_lower for word in ['add', 'feature', 'implement', 'new']):
            return 'feature'
        elif any(word in title_lower for word in ['doc', 'readme', 'documentation']):
            return 'docs'
        elif any(word in title_lower for word in ['refactor', 'cleanup', 'maintenance', 'update']):
            return 'maintenance'
        
        return 'feature' 
    
    @staticmethod
    def classify_issue_type(title: str, labels: List[str]) -> str:
        """Classify issue type based on title and labels"""
        title_lower = title.lower()
        labels_lower = [label.lower() for label in labels]
        
        if any(label in labels_lower for label in ['bug', 'error', 'broken']):
            return 'bug'
        elif any(label in labels_lower for label in ['feature', 'enhancement', 'feature-request']):
            return 'feature'
        elif any(label in labels_lower for label in ['question', 'help', 'support']):
            return 'question'
        elif any(label in labels_lower for label in ['documentation', 'docs']):
            return 'documentation'
        
        if any(word in title_lower for word in ['bug', 'error', 'broken', 'issue', 'problem']):
            return 'bug'
        elif any(word in title_lower for word in ['feature', 'request', 'add', 'implement']):
            return 'feature'
        elif any(word in title_lower for word in ['how', 'question', '?']):
            return 'question'
        elif any(word in title_lower for word in ['doc', 'documentation', 'readme']):
            return 'documentation'
        
        return 'question'  
    
    @staticmethod
    def get_priority_from_labels(labels: List[str]) -> str:
        """Extract priority from labels"""
        labels_lower = [label.lower() for label in labels]
        
        if any(label in labels_lower for label in ['critical', 'urgent', 'high-priority']):
            return 'critical'
        elif any(label in labels_lower for label in ['high', 'important']):
            return 'high'
        elif any(label in labels_lower for label in ['medium', 'normal']):
            return 'medium'
        elif any(label in labels_lower for label in ['low', 'minor']):
            return 'low'
        
        return 'medium'  
    
    @staticmethod
    def is_breaking_change(title: str, body: str, labels: List[str]) -> bool:
        """Determine if this is a breaking change"""
        text_to_check = f"{title} {body or ''}".lower()
        labels_lower = [label.lower() for label in labels]
        if any(label in labels_lower for label in ['breaking', 'breaking-change', 'major']):
            return True
        breaking_patterns = [
            r'breaking\s*change',
            r'breaking\s*api',
            r'backwards?\s*incompatible',
            r'major\s*version',
            r'removed?\s+deprecated',
            r'api\s*change'
        ]
        for pattern in breaking_patterns:
            if re.search(pattern, text_to_check):
                return True
        return False
    
    @staticmethod
    def extract_issue_numbers_from_text(text: str) -> List[int]:
        """Extract issue/PR numbers from text (e.g., 'fixes #123', 'closes #456')"""
        if not text:
            return []
        # Pattern to match #number references
        pattern = r'(?:fix(?:es)?|close(?:s)?|resolve(?:s)?|reference(?:s)?)\s*#(\d+)'
        matches = re.findall(pattern, text.lower())
        
        return [int(match) for match in matches]
    
    @staticmethod
    def calculate_time_to_merge(created_at: str, merged_at: Optional[str]) -> Optional[float]:
        """Calculate time to merge in hours"""
        if not merged_at:
            return None
        try:
            created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            merged = datetime.fromisoformat(merged_at.replace('Z', '+00:00'))
            
            delta = merged - created
            return delta.total_seconds() / 3600  # hours
        except (ValueError, AttributeError):
            return None
    
    @staticmethod
    def calculate_time_to_close(created_at: str, closed_at: Optional[str]) -> Optional[float]:
        """Calculate time to close in hours"""
        if not closed_at:
            return None
        
        try:
            created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            closed = datetime.fromisoformat(closed_at.replace('Z', '+00:00'))
            
            delta = closed - created
            return delta.total_seconds() / 3600  # hours
        except (ValueError, AttributeError):
            return None