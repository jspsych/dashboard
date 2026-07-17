"""
GitHub GraphQL API client module

Handles fetching data from the GitHub GraphQL API (v4) with the same hardening
discipline as the REST client in github_client.py: bounded timeouts, retries
with exponential backoff on network errors / 5xx, rate-limit handling, a module
logger, and loud failures that return None rather than partial data.

The GraphQL API is required for GitHub Discussions, which the REST API does not
expose. The primary entry point is `fetch_discussions(repo, github_token)`.
"""

import logging
import time

import httpx

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://api.github.com/graphql"
TIMEOUT = httpx.Timeout(30.0)
MAX_RETRIES = 3
BACKOFF_SECONDS = (2, 4, 8)
MAX_RATE_LIMIT_SLEEP = 15 * 60  # 15 minutes, in seconds


def _seconds_until_reset(reset_header):
    """Return seconds to sleep until X-RateLimit-Reset, capped at MAX_RATE_LIMIT_SLEEP."""
    try:
        reset_epoch = int(reset_header)
    except (TypeError, ValueError):
        return None
    delay = reset_epoch - time.time()
    return max(0, min(delay, MAX_RATE_LIMIT_SLEEP))


def _handle_rate_limit(response):
    """If response indicates GitHub rate limiting, sleep and return True.

    Returns False if the response was a 403/429 with no rate-limit signal
    (e.g. a permissions problem), meaning the caller should treat it as a
    hard failure rather than retrying.
    """
    retry_after = response.headers.get("Retry-After")
    if retry_after is not None:
        try:
            delay = min(float(retry_after), MAX_RATE_LIMIT_SLEEP)
        except ValueError:
            delay = None
        if delay is not None:
            logger.warning(f"Rate limited, sleeping {delay:.0f}s per Retry-After header")
            time.sleep(delay)
            return True

    remaining = response.headers.get("X-RateLimit-Remaining")
    if remaining == "0":
        delay = _seconds_until_reset(response.headers.get("X-RateLimit-Reset"))
        if delay is not None:
            logger.warning(f"Rate limit exhausted, sleeping {delay:.0f}s until reset")
            time.sleep(delay)
            return True

    return False


def _is_rate_limited_error(errors):
    """Return True if any GraphQL error is of type RATE_LIMITED."""
    if not errors:
        return False
    for err in errors:
        if isinstance(err, dict) and err.get("type") == "RATE_LIMITED":
            return True
    return False


def fetch_graphql(query, variables, github_token):
    """Execute a single GraphQL query and return its `data` payload.

    Retries on network errors, 5xx responses, HTTP rate limiting, and GraphQL
    RATE_LIMITED errors (each with a bounded sleep). Any other GraphQL error is
    logged at ERROR and results in None. Returns None on any unrecoverable
    failure; returns the `data` dict on success.
    """
    headers = {
        "Accept": "application/vnd.github.v4+json",
        "Authorization": f"bearer {github_token}",
    }
    payload = {"query": query, "variables": variables or {}}

    with httpx.Client(timeout=TIMEOUT) as client:
        attempt = 0
        while True:
            try:
                response = client.post(GRAPHQL_URL, headers=headers, json=payload)
            except httpx.RequestError as e:
                if attempt >= MAX_RETRIES:
                    logger.error(
                        f"Network error on GraphQL request after {MAX_RETRIES} retries: {e}"
                    )
                    return None
                delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
                logger.warning(
                    f"Network error on GraphQL request (attempt {attempt + 1}/{MAX_RETRIES}): "
                    f"{e}. Retrying in {delay}s"
                )
                time.sleep(delay)
                attempt += 1
                continue

            if response.status_code in (403, 429):
                if _handle_rate_limit(response):
                    # Rate-limit sleep already happened; retry without counting
                    # it against the network-error retry budget.
                    continue
                logger.error(
                    f"Hard failure (HTTP {response.status_code}) on GraphQL request: "
                    f"no rate-limit signal present, likely a permissions issue"
                )
                return None

            if response.status_code >= 500:
                if attempt >= MAX_RETRIES:
                    logger.error(
                        f"Server error (HTTP {response.status_code}) on GraphQL request "
                        f"after {MAX_RETRIES} retries"
                    )
                    return None
                delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
                logger.warning(
                    f"Server error (HTTP {response.status_code}) on GraphQL request "
                    f"(attempt {attempt + 1}/{MAX_RETRIES}). Retrying in {delay}s"
                )
                time.sleep(delay)
                attempt += 1
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error on GraphQL request: {e}")
                return None

            try:
                body = response.json()
            except ValueError as e:
                logger.error(f"Failed to decode GraphQL JSON response: {e}")
                return None

            errors = body.get("errors")
            if errors:
                if _is_rate_limited_error(errors):
                    if attempt >= MAX_RETRIES:
                        logger.error(
                            "GraphQL RATE_LIMITED error persisted after "
                            f"{MAX_RETRIES} retries"
                        )
                        return None
                    delay = _seconds_until_reset(response.headers.get("X-RateLimit-Reset"))
                    if delay is None:
                        delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
                    logger.warning(
                        f"GraphQL RATE_LIMITED (attempt {attempt + 1}/{MAX_RETRIES}); "
                        f"sleeping {delay:.0f}s before retry"
                    )
                    time.sleep(delay)
                    attempt += 1
                    continue
                logger.error(f"GraphQL query returned errors: {errors}")
                return None

            return body.get("data")


# ---------------------------------------------------------------------------
# Discussions
# ---------------------------------------------------------------------------

# Discussions are fetched 30 per page rather than 100. GitHub's GraphQL API
# rejects any query whose *maximum possible* node count exceeds 500,000; the
# nested comments(100) x replies(100) fan-out means 100 discussions/page would
# request up to 100 * 100 * 100 = 1,000,000 nodes. 30 * 100 * 100 = 300,000
# stays safely under the limit. Comments and replies remain 100/page.
_DISCUSSIONS_QUERY = """
query($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    hasDiscussionsEnabled
    discussions(first: 30, after: $cursor,
                orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        databaseId
        title
        body
        createdAt
        updatedAt
        upvoteCount
        isAnswered
        author { login }
        category { name }
        answer { databaseId createdAt }
        comments(first: 100) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            id
            databaseId
            body
            createdAt
            isAnswer
            author { login }
            replies(first: 100) {
              totalCount
              pageInfo { hasNextPage endCursor }
              nodes {
                databaseId
                body
                createdAt
                isAnswer
                author { login }
              }
            }
          }
        }
      }
    }
  }
}
"""

_COMMENTS_PAGE_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    discussion(number: $number) {
      comments(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          databaseId
          body
          createdAt
          isAnswer
          author { login }
          replies(first: 100) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              databaseId
              body
              createdAt
              isAnswer
              author { login }
            }
          }
        }
      }
    }
  }
}
"""

_REPLIES_PAGE_QUERY = """
query($commentId: ID!, $cursor: String) {
  node(id: $commentId) {
    ... on DiscussionComment {
      replies(first: 100, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          databaseId
          body
          createdAt
          isAnswer
          author { login }
        }
      }
    }
  }
}
"""


def _author_login(node):
    """Return the author login, mapping a deleted (null) author to 'ghost'."""
    author = node.get("author") if node else None
    if author and author.get("login"):
        return author["login"]
    return "ghost"


def _fetch_remaining_replies(comment, owner, name, github_token):
    """Page through a comment's replies beyond the first 100.

    The comment's `replies` node is mutated in place: additional reply nodes are
    appended to comment['replies']['nodes']. Returns True on success, False on
    an unrecoverable fetch failure (caller should abort the whole discussion).
    """
    replies = comment.get("replies") or {}
    page_info = replies.get("pageInfo") or {}
    cursor = page_info.get("endCursor")
    has_next = page_info.get("hasNextPage", False)
    if not has_next:
        return True

    comment_node_id = comment.get("id")
    fetched = 0
    while has_next:
        # Replies are paged by the parent comment's global node id. When the
        # node id is unavailable we cannot page further; bail loudly.
        if not comment_node_id:
            logger.error(
                "Cannot paginate replies for comment "
                f"{comment.get('databaseId')}: missing node id"
            )
            return False
        data = fetch_graphql(
            _REPLIES_PAGE_QUERY,
            {"commentId": comment_node_id, "cursor": cursor},
            github_token,
        )
        if data is None:
            return False
        node = data.get("node") or {}
        page = node.get("replies") or {}
        new_nodes = page.get("nodes", [])
        replies.setdefault("nodes", []).extend(new_nodes)
        fetched += len(new_nodes)
        page_info = page.get("pageInfo") or {}
        cursor = page_info.get("endCursor")
        has_next = page_info.get("hasNextPage", False)
    logger.info(
        f"Paginated {fetched} extra replies for comment {comment.get('databaseId')}"
    )
    return True


def _fetch_remaining_comments(discussion, owner, name, github_token):
    """Page through a discussion's comments beyond the first 100.

    Appends additional comment nodes to discussion['comments']['nodes'] and, for
    every comment fetched, also completes its reply pagination. Returns True on
    success, False on unrecoverable failure.
    """
    comments = discussion.get("comments") or {}
    page_info = comments.get("pageInfo") or {}
    cursor = page_info.get("endCursor")
    has_next = page_info.get("hasNextPage", False)
    number = discussion.get("number")
    fetched = 0
    while has_next:
        data = fetch_graphql(
            _COMMENTS_PAGE_QUERY,
            {"owner": owner, "name": name, "number": number, "cursor": cursor},
            github_token,
        )
        if data is None:
            return False
        repo_node = data.get("repository") or {}
        disc_node = repo_node.get("discussion") or {}
        page = disc_node.get("comments") or {}
        new_nodes = page.get("nodes", [])
        for comment in new_nodes:
            if not _fetch_remaining_replies(comment, owner, name, github_token):
                return False
        comments.setdefault("nodes", []).extend(new_nodes)
        fetched += len(new_nodes)
        page_info = page.get("pageInfo") or {}
        cursor = page_info.get("endCursor")
        has_next = page_info.get("hasNextPage", False)
    if fetched:
        logger.info(
            f"Paginated {fetched} extra comments for discussion #{number}"
        )
    return True


def fetch_discussions(repo, github_token):
    """Fetch all discussions for a repository via the GraphQL API.

    Returns a list of discussion dicts (each with fully paginated comments and
    replies). Returns [] if discussions are disabled or there are none. Returns
    None on any unrecoverable failure mid-pagination (never a truncated list).

    Args:
        repo (str): "owner/name" full repository name.
        github_token (str): A GitHub token with read access.
    """
    try:
        owner, name = repo.split("/", 1)
    except ValueError:
        logger.error(f"Invalid repo full name (expected 'owner/name'): {repo!r}")
        return None

    discussions = []
    cursor = None
    while True:
        data = fetch_graphql(
            _DISCUSSIONS_QUERY,
            {"owner": owner, "name": name, "cursor": cursor},
            github_token,
        )
        if data is None:
            logger.error(
                f"[{repo}] Discussions fetch failed mid-pagination "
                f"(after {len(discussions)} discussions); returning None"
            )
            return None

        repo_node = data.get("repository")
        if repo_node is None:
            logger.error(f"[{repo}] GraphQL returned no repository node")
            return None

        if not repo_node.get("hasDiscussionsEnabled", False):
            logger.info(f"[{repo}] Discussions are disabled; storing nothing")
            return []

        disc_conn = repo_node.get("discussions") or {}
        nodes = disc_conn.get("nodes", [])
        for discussion in nodes:
            # Complete any comment pagination (>100 comments) and, for each
            # comment, reply pagination (>100 replies) before accepting it.
            comments = discussion.get("comments") or {}
            comment_pi = comments.get("pageInfo") or {}
            if comment_pi.get("hasNextPage"):
                if not _fetch_remaining_comments(discussion, owner, name, github_token):
                    logger.error(
                        f"[{repo}] Failed paginating comments for discussion "
                        f"#{discussion.get('number')}; returning None"
                    )
                    return None
            # Reply pagination for the comments already in the first page.
            for comment in comments.get("nodes", []):
                replies = comment.get("replies") or {}
                reply_pi = replies.get("pageInfo") or {}
                if reply_pi.get("hasNextPage"):
                    if not _fetch_remaining_replies(comment, owner, name, github_token):
                        logger.error(
                            f"[{repo}] Failed paginating replies for discussion "
                            f"#{discussion.get('number')}; returning None"
                        )
                        return None
            discussions.append(discussion)

        page_info = disc_conn.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    logger.info(f"[{repo}] Fetched {len(discussions)} discussions")
    return discussions
