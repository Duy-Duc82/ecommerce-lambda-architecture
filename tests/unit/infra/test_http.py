"""HttpClient, RobotsPolicy (per host, TTL) and Throttle."""
from __future__ import annotations

import pytest

from ecomdata.infra.http import HttpClient, HttpResponse, RobotsPolicy, Throttle, parse_robots

ROBOTS = "User-agent: *\nDisallow: /private/\n\nDisallow: /api/v2/me/\n"


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls, self.closed = responses, [], False

    def get(self, url, headers, timeout):
        self.calls.append((url, headers, timeout))
        status, body = self.responses.get(url, (404, b""))
        if isinstance(status, Exception):
            raise status
        return type("R", (), {"url": url, "status_code": status, "headers": {"X-A": "1"}, "content": body})()

    def close(self):
        self.closed = True


def client_for(responses):
    session = FakeSession(responses)
    return HttpClient(user_agent="test-bot", timeout_seconds=7, session=session), session


def test_the_client_sends_its_user_agent_and_timeout_on_every_request():
    client, session = client_for({"https://a.test/x": (200, b"hello")})

    response = client.get("https://a.test/x", headers={"Accept": "text/html"})

    assert response.status_code == 200 and response.text == "hello"
    assert response.header("x-a") == "1"
    url, headers, timeout = session.calls[0]
    assert headers == {"User-Agent": "test-bot", "Accept": "text/html"} and timeout == 7
    client.close()
    assert session.closed


def test_a_non_positive_timeout_is_refused():
    with pytest.raises(ValueError, match="timeout_seconds"):
        HttpClient(user_agent="x", timeout_seconds=0)


def test_blank_lines_do_not_end_a_robots_group():
    # RobotFileParser alone drops every rule after the first blank line.
    rules = parse_robots(ROBOTS)

    assert not rules.can_fetch("bot", "https://a.test/api/v2/me/")
    assert not rules.can_fetch("bot", "https://a.test/private/x")
    assert rules.can_fetch("bot", "https://a.test/public")


def test_robots_are_fetched_once_per_host_and_cached():
    client, session = client_for({"https://a.test/robots.txt": (200, ROBOTS.encode()), "https://b.test/robots.txt": (404, b"")})
    policy = RobotsPolicy(client, user_agent="bot")

    assert policy.allowed("https://a.test/public")
    assert not policy.allowed("https://a.test/private/1")
    assert policy.allowed("https://b.test/private/1")  # no robots.txt: no restriction
    assert policy.allowed("https://a.test/another")

    assert policy.fetches == 2
    assert [call[0] for call in session.calls] == ["https://a.test/robots.txt", "https://b.test/robots.txt"]


def test_robots_are_refetched_after_the_ttl():
    now = [0.0]
    client, session = client_for({"https://a.test/robots.txt": (200, ROBOTS.encode())})
    policy = RobotsPolicy(client, user_agent="bot", ttl_seconds=60, clock=lambda: now[0])

    policy.allowed("https://a.test/x")
    now[0] = 59
    policy.allowed("https://a.test/x")
    now[0] = 61
    policy.allowed("https://a.test/x")

    assert policy.fetches == 2


@pytest.mark.parametrize("outcome", [(403, b""), (401, b""), (ConnectionError("down"), b"")])
def test_an_unreadable_robots_file_denies(outcome):
    client, _ = client_for({"https://a.test/robots.txt": outcome})

    assert RobotsPolicy(client, user_agent="bot").allowed("https://a.test/x") is False


def test_a_parse_only_policy_denies_without_a_client():
    assert RobotsPolicy.deny_all().allowed("https://a.test/x") is False


def test_the_throttle_waits_per_host_with_the_injected_clock():
    now, slept = [100.0], []
    throttle = Throttle(min_interval_seconds=2, sleep=slept.append, clock=lambda: now[0])

    assert throttle.wait("https://a.test/1") == 0.0  # first request to a host never waits
    now[0] = 100.5
    assert throttle.wait("https://a.test/2") == pytest.approx(1.5)
    assert throttle.wait("https://b.test/1") == 0.0  # another host has its own clock
    now[0] = 110
    assert throttle.wait("https://a.test/3") == 0.0
    assert slept == [pytest.approx(1.5)]


def test_a_per_call_interval_can_only_lengthen_the_wait():
    now, slept = [0.0], []
    throttle = Throttle(min_interval_seconds=2, sleep=slept.append, clock=lambda: now[0])

    throttle.wait("https://a.test/1")
    throttle.wait("https://a.test/2", min_interval_seconds=5)
    throttle.wait("https://a.test/3", min_interval_seconds=1)

    assert slept == [5, 2]


def test_jitter_adds_to_the_interval():
    now, slept = [0.0], []
    throttle = Throttle(
        min_interval_seconds=2, jitter_seconds=1, sleep=slept.append, clock=lambda: now[0], jitter_source=lambda a, b: b
    )

    throttle.wait("https://a.test/1")
    throttle.wait("https://a.test/2")

    assert slept == [3]


def test_a_negative_interval_is_refused():
    with pytest.raises(ValueError):
        Throttle(min_interval_seconds=-1)


def test_a_response_header_lookup_ignores_case():
    response = HttpResponse("https://a.test", 200, {"Retry-After": "9"}, b"", 3)

    assert response.header("retry-after") == "9" and response.header("missing") is None
