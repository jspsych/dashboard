import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .config import Config
from .database import DatabaseManager
from .github_client import fetch_api
from .graphql_client import fetch_discussions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
GITHUB_TOKEN = Config.GITHUB_TOKEN

class GitHubDataPipeline:
    """Main pipeline for fetching and storing data for a single repository."""
    def __init__(self, repo: str, db_path: str = "data/analytics.db"):
        self.repo = repo
        self.db = DatabaseManager(db_path)
        self.github_token = GITHUB_TOKEN

        if not self.github_token:
            raise ValueError("GITHUB_TOKEN not found in environment variables")

    def fetch_and_store_pull_requests(self, state: str = "all") -> int:
        """Fetch pull requests from GitHub and store in database"""
        logger.info(f"[{self.repo}] Fetching pull requests with state: {state}")
        endpoint = f"pulls?state={state}&per_page=100&sort=updated&direction=desc"
        pr_data = fetch_api(self.repo, endpoint, self.github_token)
        if not pr_data:
            logger.error("Failed to fetch pull request data")
            return 0
        stored_count = 0
        for pr in pr_data:
            processed_pr = self._process_pull_request(pr)
            if self.db.upsert_pull_request(processed_pr):
                stored_count += 1
        self.db.update_last_sync_time('pr', self.repo)
        self.db.set_metadata(f'total_prs_tracked:{self.repo}', str(stored_count))
        logger.info(f"[{self.repo}] Stored {stored_count} pull requests")
        return stored_count

    def fetch_and_store_pull_requests_since(self, since_iso: Optional[str]) -> int:
        """Incrementally fetch pull requests updated since the given ISO timestamp.
        """
        logger.info(f"[{self.repo}] Incremental fetch for pull requests...")
        endpoint = "pulls?state=all&per_page=100&sort=updated&direction=desc"
        pr_data = fetch_api(self.repo, endpoint, self.github_token)
        if not pr_data:
            logger.error("Failed to fetch pull request data (incremental)")
            return 0
        stored_count = 0
        for pr in pr_data:
            if since_iso and pr.get('updated_at') and pr['updated_at'] <= since_iso:
                break
            processed_pr = self._process_pull_request(pr)
            if self.db.upsert_pull_request(processed_pr):
                stored_count += 1
        logger.info(f"[{self.repo}] Incremental stored {stored_count} pull requests")
        return stored_count

    def fetch_add_del_data(self) -> int:
        """Fetch additions and deletions for all PRs"""
        logger.info(f"[{self.repo}] Fetching additions and deletions for all PRs...")
        pull_requests = self.db.get_pull_requests(self.repo)
        count = 0
        for pr in pull_requests:
            pr_number = pr['number']
            logger.info(f"[{self.repo}] Fetching stats for PR #{pr_number}")
            endpoint = f"pulls/{pr_number}"
            pr_data = fetch_api(self.repo, endpoint, self.github_token)
            if pr_data and isinstance(pr_data, dict):
                additions = pr_data.get('additions', 0)
                deletions = pr_data.get('deletions', 0)
                changed_files = pr_data.get('changed_files', 0)
                commits_count = pr_data.get('commits', 0)
                updated_pr = dict(pr)
                updated_pr['additions'] = additions
                updated_pr['deletions'] = deletions
                updated_pr['changed_files'] = changed_files
                updated_pr['commits_count'] = commits_count
                self.db.upsert_pull_request(updated_pr)
                print(f"PR #{pr_number}: +{additions} -{deletions} ({changed_files} files changed, {commits_count} commits)")
                count += 1
            else:
                print(f"Failed to fetch data for PR #{pr_number}")
        logger.info("Completed fetching additions and deletions.")
        return count

    def fetch_add_del_for_prs(self, pr_numbers: List[int]) -> int:
        """Fetch additions and deletions for specified PR numbers only (incremental)."""
        if not pr_numbers:
            return 0
        logger.info(f"[{self.repo}] Fetching additions/deletions for {len(pr_numbers)} PRs (incremental)...")
        count = 0
        existing_prs = {pr['number']: pr for pr in self.db.get_pull_requests(self.repo)}
        for pr_number in pr_numbers:
            endpoint = f"pulls/{pr_number}"
            pr_data = fetch_api(self.repo, endpoint, self.github_token)
            if pr_data and isinstance(pr_data, dict):
                additions = pr_data.get('additions', 0)
                deletions = pr_data.get('deletions', 0)
                changed_files = pr_data.get('changed_files', 0)
                commits_count = pr_data.get('commits', 0)
                base = existing_prs.get(pr_number)
                if base is None:
                    processed_pr = self._process_pull_request(pr_data)
                    processed_pr['additions'] = additions
                    processed_pr['deletions'] = deletions
                    processed_pr['changed_files'] = changed_files
                    processed_pr['commits_count'] = commits_count
                    self.db.upsert_pull_request(processed_pr)
                else:
                    updated_pr = dict(base)
                    updated_pr['additions'] = additions
                    updated_pr['deletions'] = deletions
                    updated_pr['changed_files'] = changed_files
                    updated_pr['commits_count'] = commits_count
                    self.db.upsert_pull_request(updated_pr)
                count += 1
        logger.info(f"[{self.repo}] Incremental additions/deletions updated for {count} PRs")
        return count

    def fetch_and_store_issues(self, state: str = "all") -> int:
        """Fetch issues from GitHub and store in database"""
        logger.info(f"[{self.repo}] Fetching issues with state: {state}")
        endpoint = f"issues?state={state}&per_page=100&sort=updated&direction=desc"
        issue_data = fetch_api(self.repo, endpoint, self.github_token)
        if not issue_data:
            logger.error("Failed to fetch issue data")
            return 0
        stored_count = 0
        for issue in issue_data:
            if 'pull_request' not in issue:
                processed_issue = self._process_issue(issue)
                if self.db.upsert_issue(processed_issue):
                    stored_count += 1
        self.db.update_last_sync_time('issue', self.repo)
        self.db.set_metadata(f'total_issues_tracked:{self.repo}', str(stored_count))
        logger.info(f"[{self.repo}] Stored {stored_count} issues")
        return stored_count

    def fetch_and_store_issues_since(self, since_iso: Optional[str]) -> int:
        """Incrementally fetch issues updated since the given ISO timestamp."""
        logger.info(f"[{self.repo}] Incremental fetch for issues...")
        endpoint = "issues?state=all&per_page=100&sort=updated&direction=desc"
        if since_iso:
            endpoint += f"&since={since_iso}"
        issue_data = fetch_api(self.repo, endpoint, self.github_token)
        if not issue_data:
            logger.error("Failed to fetch issue data (incremental)")
            return 0
        stored_count = 0
        for issue in issue_data:
            if 'pull_request' not in issue:
                processed_issue = self._process_issue(issue)
                if self.db.upsert_issue(processed_issue):
                    stored_count += 1
        logger.info(f"[{self.repo}] Incremental stored {stored_count} issues")
        return stored_count

    def fetch_and_store_reviews_for_all_prs(self) -> int:
        """Fetch reviews for all pull requests in the database."""
        logger.info(f"[{self.repo}] Fetching reviews for all PRs...")
        pull_requests = self.db.get_pull_requests(self.repo)
        total_reviews_stored = 0
        for pr in pull_requests:
            pr_number = pr['number']
            logger.info(f"[{self.repo}] Fetching reviews for PR #{pr_number}")
            endpoint = f"pulls/{pr_number}/reviews"
            reviews_data = fetch_api(self.repo, endpoint, self.github_token)
            if reviews_data:
                for review in reviews_data:
                    processed_review = self._process_review(review, pr_number)
                    if self.db.upsert_review(processed_review):
                        total_reviews_stored += 1
        logger.info(f"[{self.repo}] Stored {total_reviews_stored} reviews in total.")
        return total_reviews_stored

    def fetch_and_store_reviews_for_prs(self, pr_numbers: List[int]) -> int:
        """Fetch reviews only for the provided PR numbers (incremental path)."""
        if not pr_numbers:
            return 0
        logger.info(f"[{self.repo}] Fetching reviews for {len(pr_numbers)} PRs (incremental)...")
        total_reviews_stored = 0
        for pr_number in pr_numbers:
            endpoint = f"pulls/{pr_number}/reviews"
            reviews_data = fetch_api(self.repo, endpoint, self.github_token)
            if reviews_data:
                for review in reviews_data:
                    processed_review = self._process_review(review, pr_number)
                    if self.db.upsert_review(processed_review):
                        total_reviews_stored += 1
        logger.info(f"[{self.repo}] Incremental stored {total_reviews_stored} reviews")
        return total_reviews_stored

    def fetch_and_store_comments(self) -> int:
        """Fetch all issue and PR comments."""
        logger.info(f"[{self.repo}] Fetching all comments...")
        # Note: This fetches repository-wide issue comments, which includes PR comments.
        endpoint = "issues/comments?sort=updated&direction=desc&per_page=100"
        comments_data = fetch_api(self.repo, endpoint, self.github_token)
        if not comments_data:
            logger.warning("No comments found or failed to fetch.")
            return 0
        
        stored_count = 0
        for comment in comments_data:
            processed_comment = self._process_comment(comment)
            if self.db.upsert_comment(processed_comment):
                stored_count += 1
        logger.info(f"[{self.repo}] Stored {stored_count} comments.")
        return stored_count

    def fetch_and_store_releases(self) -> int:
        """Fetch all releases for the repository."""
        logger.info(f"[{self.repo}] Fetching releases...")
        endpoint = "releases?per_page=100"
        releases_data = fetch_api(self.repo, endpoint, self.github_token)
        if not releases_data:
            logger.warning("No releases found or failed to fetch.")
            return 0
        
        stored_count = 0
        for release in releases_data:
            processed_release = self._process_release(release)
            if self.db.upsert_release(processed_release):
                stored_count += 1
        logger.info(f"[{self.repo}] Stored {stored_count} releases.")
        return stored_count

    def fetch_and_store_discussions(self) -> int:
        """Fetch GitHub Discussions via GraphQL and store them plus their
        comments and replies.

        Discussions and their comment threads are fetched through the GraphQL
        API (the REST API cannot see them). Each discussion is flattened into
        the `discussions` table; every top-level comment and every reply is
        flattened into `discussion_comments` (replies carry the parent comment's
        databaseId in parent_comment_id and share the discussion_number).

        A repo with discussions disabled (or zero discussions) stores nothing
        and is not an error. On an unrecoverable fetch failure this returns 0
        and does not update the sync marker.

        Returns the number of discussions stored.
        """
        logger.info(f"[{self.repo}] Fetching discussions via GraphQL...")
        discussions = fetch_discussions(self.repo, self.github_token)
        if discussions is None:
            logger.error(f"[{self.repo}] Failed to fetch discussions")
            return 0

        stored_count = 0
        comment_count = 0
        for disc in discussions:
            comments = (disc.get('comments') or {}).get('nodes', [])
            # comments_count = top-level comments + all their replies
            total_comments = 0
            for comment in comments:
                total_comments += 1
                total_comments += len((comment.get('replies') or {}).get('nodes', []))

            answer = disc.get('answer') or {}
            disc_record = {
                'id': disc['databaseId'],
                'repo': self.repo,
                'number': disc['number'],
                'title': disc['title'],
                'body': disc.get('body'),
                'category': (disc.get('category') or {}).get('name'),
                'created_at': disc['createdAt'],
                'updated_at': disc['updatedAt'],
                'author_login': self._discussion_author(disc),
                'is_answered': disc.get('isAnswered', False),
                'answer_comment_id': answer.get('databaseId'),
                'answer_created_at': answer.get('createdAt'),
                'upvote_count': disc.get('upvoteCount'),
                'comments_count': total_comments,
            }
            if self.db.upsert_discussion(disc_record):
                stored_count += 1

            number = disc['number']
            for comment in comments:
                comment_record = {
                    'id': comment['databaseId'],
                    'repo': self.repo,
                    'discussion_number': number,
                    'author_login': self._discussion_author(comment),
                    'body': comment.get('body'),
                    'created_at': comment['createdAt'],
                    'is_answer': comment.get('isAnswer', False),
                    'parent_comment_id': None,
                }
                if self.db.upsert_discussion_comment(comment_record):
                    comment_count += 1
                for reply in (comment.get('replies') or {}).get('nodes', []):
                    reply_record = {
                        'id': reply['databaseId'],
                        'repo': self.repo,
                        'discussion_number': number,
                        'author_login': self._discussion_author(reply),
                        'body': reply.get('body'),
                        'created_at': reply['createdAt'],
                        'is_answer': reply.get('isAnswer', False),
                        'parent_comment_id': comment['databaseId'],
                    }
                    if self.db.upsert_discussion_comment(reply_record):
                        comment_count += 1

        self.db.update_last_sync_time('discussion', self.repo)
        self.db.set_metadata(f'total_discussions_tracked:{self.repo}', str(stored_count))
        logger.info(
            f"[{self.repo}] Stored {stored_count} discussions and {comment_count} discussion comments"
        )
        return stored_count

    @staticmethod
    def _discussion_author(node: Dict[str, Any]) -> str:
        """Return a node's author login, mapping deleted (null) authors to 'ghost'."""
        author = node.get('author') if node else None
        if author and author.get('login'):
            return author['login']
        return 'ghost'

    def _process_pull_request(self, pr: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw PR data from GitHub API"""
        labels = [label['name'] for label in pr.get('labels', [])]
        assignees = [assignee['login'] for assignee in pr.get('assignees', [])]
        return {
            'id': pr['id'],
            'repo': self.repo,
            'number': pr['number'],
            'title': pr['title'],
            'body': pr.get('body'),
            'state': 'merged' if pr.get('merged_at') else pr['state'],
            'created_at': pr['created_at'],
            'updated_at': pr['updated_at'],
            'closed_at': pr.get('closed_at'),
            'merged_at': pr.get('merged_at'),
            'user_login': pr['user']['login'],
            'user_type': pr['user'].get('type'),
            'base_branch': pr['base']['ref'],
            'head_branch': pr['head']['ref'],
            'additions': pr.get('additions'),
            'deletions': pr.get('deletions'),
            'changed_files': pr.get('changed_files'),
            'commits_count': pr.get('commits'),
            'labels': labels,
            'assignees': assignees,
            'draft': pr.get('draft', False),
            'mergeable': pr.get('mergeable'),
        }
    
    def _process_issue(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw issue data from GitHub API"""
        labels = [label['name'] for label in issue.get('labels', [])]
        assignee = issue.get('assignee')
        return {
            'id': issue['id'],
            'repo': self.repo,
            'number': issue['number'],
            'title': issue['title'],
            'body': issue.get('body'),
            'state': issue['state'],
            'created_at': issue['created_at'],
            'updated_at': issue['updated_at'],
            'closed_at': issue.get('closed_at'),
            'user_login': issue['user']['login'],
            'user_type': issue['user'].get('type'),
            'assignee_login': assignee['login'] if assignee else None,
            'labels': labels,
            'comments_count': issue.get('comments'),
            'is_external_user': issue['user'].get('type') == 'User'
        }
    
    def _process_review(self, review: Dict[str, Any], pr_number: int) -> Dict[str, Any]:
        """Process raw review data from GitHub API"""
        return {
            'id': review['id'],
            'repo': self.repo,
            'pr_number': pr_number,
            'reviewer_login': review['user']['login'],
            'state': review['state'],
            'submitted_at': review['submitted_at'],
            'body': review.get('body'),
            'commit_sha': review.get('commit_id'),
        }

    def _process_comment(self, comment: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw comment data from GitHub API"""
        issue_url = comment['issue_url']
        number = int(issue_url.split('/')[-1])
        # Cross-reference with pull_requests table to determine if this is a PR comment
        pr = self.db.get_pull_request_by_number(self.repo, number)
        is_pr = pr is not None
        return {
            'id': comment['id'],
            'repo': self.repo,
            'issue_number': None if is_pr else number,
            'pr_number': number if is_pr else None,
            'user_login': comment['user']['login'],
            'body': comment.get('body'),
            'created_at': comment['created_at'],
            'updated_at': comment['updated_at'],
            'comment_type': 'pr' if is_pr else 'issue',
        }

    def _process_release(self, release: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw release data from GitHub API"""
        return {
            'id': release['id'],
            'repo': self.repo,
            'tag_name': release['tag_name'],
            'name': release.get('name'),
            'body': release.get('body'),
            'created_at': release['created_at'],
            'published_at': release.get('published_at'),
            'draft': release.get('draft', False),
            'prerelease': release.get('prerelease', False),
            'author_login': release['author']['login'],
            'tarball_url': release.get('tarball_url'),
            'zipball_url': release.get('zipball_url'),
            'is_breaking': 'breaking' in (release.get('name', '').lower() + release.get('body', '').lower()),
        }

    def sync_all_data(self):
        """Comprehensive sync of all GitHub data"""
        logger.info(f"[{self.repo}] Starting full data sync...")

        pr_count = self.fetch_and_store_pull_requests()
        issue_count = self.fetch_and_store_issues()
        review_count = self.fetch_and_store_reviews_for_all_prs()
        comment_count = self.fetch_and_store_comments()
        release_count = self.fetch_and_store_releases()
        discussion_count = self.fetch_and_store_discussions()
        add_count = self.fetch_add_del_data()

        self.db.update_last_sync_time('full', self.repo)
        logger.info(f"[{self.repo}] Full sync completed: {pr_count} PRs, {issue_count} issues, {review_count} reviews, {release_count} releases, {comment_count} comments, and {discussion_count} discussions stored.")
        return {
            'pull_requests': pr_count,
            'issues': issue_count,
            'reviews': review_count,
            'comments': comment_count,
            'releases': release_count,
            'discussions': discussion_count,
            'additions_deletions_fetched': add_count,
            'timestamp': datetime.utcnow().isoformat()
        }

    def sync_incremental(self):
        """Incremental sync of PRs, issues, and additions/deletions"""
        logger.info(f"[{self.repo}] Starting incremental data sync (PRs, issues, reviews)...")
        from datetime import datetime
        last_inc_raw = self.db.get_sync_time_value('incremental', self.repo)
        last_full_raw = self.db.get_sync_time_value('full', self.repo)
        default_time = "2000-01-01T00:00:00"
        last_inc_dt = datetime.fromisoformat(last_inc_raw) if last_inc_raw else datetime.fromisoformat(default_time)
        last_full_dt = datetime.fromisoformat(last_full_raw) if last_full_raw else datetime.fromisoformat(default_time)
        last_inc = max(last_inc_dt, last_full_dt).isoformat()
        pr_count = self.fetch_and_store_pull_requests_since(last_inc)
        updated_pr_numbers: List[int] = []
        endpoint = "pulls?state=all&per_page=100&sort=updated&direction=desc"
        pr_data = fetch_api(self.repo, endpoint, self.github_token)
        if pr_data:
            for pr in pr_data:
                if not last_inc or (pr.get('updated_at') and pr['updated_at'] > last_inc):
                    updated_pr_numbers.append(pr['number'])
                else:
                    break
        issue_count = self.fetch_and_store_issues_since(last_inc)
        add_del_count = self.fetch_add_del_for_prs(updated_pr_numbers)
        # Discussions GraphQL has no reliable `since` filter, so incremental
        # mode performs a full refetch of discussions like full sync does.
        discussion_count = self.fetch_and_store_discussions()
        self.db.update_last_sync_time('incremental', self.repo)
        logger.info(
            f"[{self.repo}] Incremental sync completed: {pr_count} PRs, {issue_count} issues, "
            f"{add_del_count} PR stats, {discussion_count} discussions (full refetch)."
        )
        return {
            'pull_requests': pr_count,
            'issues': issue_count,
            'additions_deletions_updated': add_del_count,
            'discussions': discussion_count,
            'timestamp': datetime.utcnow().isoformat()
        }
