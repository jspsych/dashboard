"""
GitHub API client module
Handles fetching data from GitHub API with pagination support
"""

import logging
import time

import httpx

logger = logging.getLogger(__name__)

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


def _request_with_retries(client, url, headers, endpoint):
    """Perform a GET request, retrying on network errors, 5xx, and rate limits.

    Returns an httpx.Response on success, or None if retries were exhausted
    or a hard (non-retryable) failure occurred.
    """
    attempt = 0
    while True:
        try:
            response = client.get(url, headers=headers)
        except httpx.RequestError as e:
            if attempt >= MAX_RETRIES:
                logger.error(
                    f"Network error fetching {endpoint} after {MAX_RETRIES} retries: {e}"
                )
                return None
            delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            logger.warning(
                f"Network error fetching {endpoint} (attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                f"Retrying in {delay}s"
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
                f"Hard failure (HTTP {response.status_code}) fetching {endpoint}: "
                f"no rate-limit signal present, likely a permissions issue"
            )
            return None

        if response.status_code >= 500:
            if attempt >= MAX_RETRIES:
                logger.error(
                    f"Server error (HTTP {response.status_code}) fetching {endpoint} "
                    f"after {MAX_RETRIES} retries"
                )
                return None
            delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            logger.warning(
                f"Server error (HTTP {response.status_code}) fetching {endpoint} "
                f"(attempt {attempt + 1}/{MAX_RETRIES}). Retrying in {delay}s"
            )
            time.sleep(delay)
            attempt += 1
            continue

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching {endpoint}: {e}")
            return None

        return response


def fetch_api(endpoint, github_token):
    url = f"https://api.github.com/repos/jspsych/jsPsych/{endpoint}"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f'token {github_token}'
    }
    with httpx.Client(timeout=TIMEOUT) as client:
        response = _request_with_retries(client, url, headers, endpoint)
        if response is None:
            return None

        data = response.json()
        if not isinstance(data, list):
            return data

        final_data = list(data)
        while 'next' in response.links:
            next_url = response.links['next']['url']
            response = _request_with_retries(client, next_url, headers, endpoint)
            if response is None:
                logger.error(
                    f"Pagination failed part-way through {endpoint}; "
                    f"discarding partial results ({len(final_data)} items) and returning None"
                )
                return None
            data = response.json()
            if not isinstance(data, list):
                break
            final_data.extend(data)
        return final_data
