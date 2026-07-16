"""
HTTP client tests (no network). httpx.Client is replaced with a fake that
returns a scripted sequence of responses/exceptions, and time.sleep is stubbed
so retry/backoff paths run instantly.

Covers the REST client (500-then-200 retry, 403+Retry-After sleep-retry,
persistent failure -> None, mid-pagination failure -> None not a truncated
list) and the GraphQL client (RATE_LIMITED retried, other GraphQL errors ->
None, discussions-disabled -> []).
"""

import httpx
import pytest

from src import github_client, graphql_client


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, headers=None, links=None):
        self.status_code = status_code
        self._json = json_data
        self.headers = headers or {}
        self.links = links or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "error",
                request=httpx.Request("GET", "http://x"),
                response=httpx.Response(self.status_code),
            )


class FakeClient:
    def __init__(self, actions):
        self.actions = list(actions)
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def _next(self, url):
        self.calls.append(url)
        assert self.actions, "FakeClient ran out of scripted actions"
        action = self.actions.pop(0)
        if isinstance(action, Exception):
            raise action
        return action

    def get(self, url, headers=None):
        return self._next(url)

    def post(self, url, headers=None, json=None):
        return self._next(url)


@pytest.fixture
def install(monkeypatch):
    """Install a scripted FakeClient + stubbed sleep on a client module."""
    sleeps = []

    def _install(module, actions):
        monkeypatch.setattr(module.httpx, "Client", lambda *a, **k: FakeClient(actions))
        monkeypatch.setattr(module.time, "sleep", lambda s: sleeps.append(s))
        return sleeps

    return _install


# --- REST client -----------------------------------------------------------

def test_rest_500_then_200_retry(install):
    sleeps = install(github_client, [
        FakeResponse(500),
        FakeResponse(200, json_data=[{"a": 1}]),
    ])
    result = github_client.fetch_api("owner/repo", "pulls", "tok")
    assert result == [{"a": 1}]
    assert sleeps  # a backoff sleep happened


def test_rest_403_retry_after_sleep(install):
    sleeps = install(github_client, [
        FakeResponse(403, headers={"Retry-After": "1"}),
        FakeResponse(200, json_data=[1, 2]),
    ])
    result = github_client.fetch_api("owner/repo", "pulls", "tok")
    assert result == [1, 2]
    assert 1.0 in sleeps


def test_rest_rate_limit_remaining_zero(install):
    import time as _time
    reset = str(int(_time.time()) + 5)
    sleeps = install(github_client, [
        FakeResponse(403, headers={"X-RateLimit-Remaining": "0",
                                   "X-RateLimit-Reset": reset}),
        FakeResponse(200, json_data=[1]),
    ])
    assert github_client.fetch_api("owner/repo", "pulls", "tok") == [1]
    assert sleeps


def test_rest_persistent_network_error_returns_none(install):
    install(github_client, [httpx.ConnectError("boom")] * 4)
    assert github_client.fetch_api("owner/repo", "pulls", "tok") is None


def test_rest_hard_403_returns_none(install):
    # 403 with no rate-limit signal -> permissions problem, hard failure.
    install(github_client, [FakeResponse(403, headers={})])
    assert github_client.fetch_api("owner/repo", "pulls", "tok") is None


def test_rest_pagination_failure_returns_none_not_partial(install):
    install(github_client, [
        FakeResponse(200, json_data=[1, 2],
                     links={"next": {"url": "http://api/next"}}),
        FakeResponse(403, headers={}),  # hard failure mid-pagination
    ])
    # Must discard the partial first page and return None.
    assert github_client.fetch_api("owner/repo", "pulls", "tok") is None


def test_rest_success_paginates(install):
    install(github_client, [
        FakeResponse(200, json_data=[1, 2],
                     links={"next": {"url": "http://api/next"}}),
        FakeResponse(200, json_data=[3, 4]),
    ])
    assert github_client.fetch_api("owner/repo", "pulls", "tok") == [1, 2, 3, 4]


def test_rest_non_list_payload_returned_directly(install):
    install(github_client, [FakeResponse(200, json_data={"k": "v"})])
    assert github_client.fetch_api("owner/repo", "x", "tok") == {"k": "v"}


# --- GraphQL client --------------------------------------------------------

def test_graphql_rate_limited_error_retried(install):
    sleeps = install(graphql_client, [
        FakeResponse(200, json_data={"errors": [{"type": "RATE_LIMITED"}]}),
        FakeResponse(200, json_data={"data": {"ok": 1}}),
    ])
    assert graphql_client.fetch_graphql("q", {}, "tok") == {"ok": 1}
    assert sleeps


def test_graphql_other_error_returns_none(install):
    install(graphql_client, [
        FakeResponse(200, json_data={"errors": [{"message": "bad field"}]}),
    ])
    assert graphql_client.fetch_graphql("q", {}, "tok") is None


def test_graphql_500_then_success(install):
    install(graphql_client, [
        FakeResponse(500),
        FakeResponse(200, json_data={"data": {"x": 1}}),
    ])
    assert graphql_client.fetch_graphql("q", {}, "tok") == {"x": 1}


def test_graphql_persistent_rate_limited_returns_none(install):
    install(graphql_client, [
        FakeResponse(200, json_data={"errors": [{"type": "RATE_LIMITED"}]}),
    ] * 4)
    assert graphql_client.fetch_graphql("q", {}, "tok") is None


# --- fetch_discussions (fetch_graphql mocked directly) ---------------------

def test_fetch_discussions_disabled_returns_empty(monkeypatch):
    monkeypatch.setattr(
        graphql_client, "fetch_graphql",
        lambda q, v, t: {"repository": {"hasDiscussionsEnabled": False}},
    )
    assert graphql_client.fetch_discussions("owner/repo", "tok") == []


def test_fetch_discussions_none_on_failure(monkeypatch):
    monkeypatch.setattr(graphql_client, "fetch_graphql", lambda q, v, t: None)
    assert graphql_client.fetch_discussions("owner/repo", "tok") is None


def test_fetch_discussions_invalid_repo(monkeypatch):
    assert graphql_client.fetch_discussions("no-slash", "tok") is None


def test_fetch_discussions_success(monkeypatch):
    data = {
        "repository": {
            "hasDiscussionsEnabled": True,
            "discussions": {
                "pageInfo": {"hasNextPage": False, "endCursor": None},
                "nodes": [
                    {
                        "number": 1,
                        "comments": {
                            "pageInfo": {"hasNextPage": False},
                            "nodes": [],
                        },
                    }
                ],
            },
        }
    }
    monkeypatch.setattr(graphql_client, "fetch_graphql", lambda q, v, t: data)
    result = graphql_client.fetch_discussions("owner/repo", "tok")
    assert len(result) == 1
    assert result[0]["number"] == 1
