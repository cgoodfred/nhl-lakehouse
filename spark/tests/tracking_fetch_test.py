"""Tests for bronze.tracking_ingest.fetch_tracking and the rate-limit / 429
backoff infrastructure (TokenBucket, backoff_delay).

The load-bearing invariants:
  * A 200 response with a body that ISN'T a non-empty top-level JSON list
    must NOT be reported as success. Otherwise the job would PUT garbage
    (Cloudflare interstitials, HTML errors, empty arrays) to durable bronze.
  * A 429 inside the retry budget triggers a backoff + retry, not a status.
  * A 429 exhausting the retry budget falls through to http_other so the
    job-level --retry-transient flag can pick it up on a future run.
  * Retry-After header is honored when present (seconds form).

requests.get is monkeypatched per-test — faster than network mocks and
forces each test to declare exactly what HTTP shape it's exercising.
TokenBucket and backoff_delay tests inject fake clocks / sleeps so they
don't burn wall-clock time.
"""

from types import SimpleNamespace

import pytest
import requests

import tracking_ingest
from tracking_ingest import TokenBucket, backoff_delay, fetch_tracking


def _resp(status_code, content=b"", text="", headers=None):
    """Minimal stand-in for a requests.Response."""
    return SimpleNamespace(
        status_code=status_code, content=content, text=text,
        headers=headers or {},
    )


@pytest.fixture
def patch_get(monkeypatch):
    """Install a fake requests.get returning the given response or raising
    the given exception. Pass a list to step through multiple responses."""
    def _patch(resp_or_exc):
        responses = resp_or_exc if isinstance(resp_or_exc, list) else [resp_or_exc]
        idx = {"i": 0}

        def fake_get(*args, **kwargs):
            r = responses[min(idx["i"], len(responses) - 1)]
            idx["i"] += 1
            if isinstance(r, Exception):
                raise r
            return r
        monkeypatch.setattr(tracking_ingest.requests, "get", fake_get)
    return _patch


def test_success_on_200_with_non_empty_list(patch_get):
    body = b'[{"timeStamp": 1, "onIce": {}}, {"timeStamp": 2, "onIce": {}}]'
    patch_get(_resp(200, content=body))
    result = fetch_tracking("https://x", {})
    assert result.status == "success"
    assert result.http_code == 200
    assert result.body == body
    assert result.frame_count == 2
    assert result.error is None


def test_invalid_payload_on_200_with_empty_list(patch_get):
    # A 200 + valid JSON empty list is still useless to us — silver would
    # produce zero frames for the goal. Treat as invalid so it's retried.
    patch_get(_resp(200, content=b"[]"))
    result = fetch_tracking("https://x", {})
    assert result.status == "invalid_payload"
    assert result.body is None
    assert result.frame_count is None
    assert "empty list" in (result.error or "")


def test_invalid_payload_on_200_with_dict(patch_get):
    # An object instead of a list — wrong shape entirely.
    patch_get(_resp(200, content=b'{"frames": []}'))
    result = fetch_tracking("https://x", {})
    assert result.status == "invalid_payload"
    assert "got dict" in (result.error or "")


def test_invalid_payload_on_200_with_html(patch_get):
    # The Cloudflare-challenge case: 200 status, HTML body. Must NOT be
    # written to bronze as if it were tracking JSON.
    patch_get(_resp(200, content=b"<html><body>Just a moment...</body></html>"))
    result = fetch_tracking("https://x", {})
    assert result.status == "invalid_payload"
    assert result.body is None
    assert "JSON parse" in (result.error or "")


def test_http_404_on_404(patch_get):
    patch_get(_resp(404, text="Not Found"))
    result = fetch_tracking("https://x", {})
    assert result.status == "http_404"
    assert result.http_code == 404
    assert result.body is None
    assert result.frame_count is None


def test_http_other_on_500(patch_get):
    patch_get(_resp(500, text="Internal Server Error"))
    result = fetch_tracking("https://x", {})
    assert result.status == "http_other"
    assert result.http_code == 500
    assert result.body is None
    # The error preview is truncated to the first 200 chars of the body.
    assert result.error == "Internal Server Error"


def test_fetch_error_on_request_exception(patch_get):
    patch_get(requests.ConnectionError("connection refused"))
    result = fetch_tracking("https://x", {})
    assert result.status == "fetch_error"
    assert result.http_code is None
    assert "ConnectionError" in (result.error or "")


# ---------- 429 backoff + retry -------------------------------------------


def test_429_then_success_returns_success(patch_get):
    body = b'[{"timeStamp": 1, "onIce": {}}]'
    patch_get([
        _resp(429, text="rate limited"),
        _resp(200, content=body),
    ])
    sleeps: list[float] = []
    result = fetch_tracking(
        "https://x", {}, max_retries=3,
        backoff_fn=lambda *_: 0.0, _sleep=sleeps.append,
    )
    assert result.status == "success"
    assert result.frame_count == 1
    # One backoff sleep should have happened between the 429 and the 200.
    assert sleeps == [0.0]


def test_429_exhausted_returns_http_other(patch_get):
    patch_get(_resp(429, text="still rate limited"))
    sleeps: list[float] = []
    result = fetch_tracking(
        "https://x", {}, max_retries=3,
        backoff_fn=lambda *_: 0.0, _sleep=sleeps.append,
    )
    assert result.status == "http_other"
    assert result.http_code == 429
    assert "exhausted 3 retries" in (result.error or "")
    # Three backoff sleeps for the three retries; the fourth attempt fell
    # through to http_other without sleeping.
    assert sleeps == [0.0, 0.0, 0.0]


def test_429_honors_retry_after_seconds(patch_get):
    patch_get([
        _resp(429, headers={"Retry-After": "7"}),
        _resp(200, content=b'[{"timeStamp": 1, "onIce": {}}]'),
    ])
    sleeps: list[float] = []
    fetch_tracking(
        "https://x", {}, max_retries=3,
        _sleep=sleeps.append,
    )
    # The 429 used the real backoff_delay default; with Retry-After=7 it
    # should pick 7s instead of attempt-0's 1s exponential value.
    assert sleeps == [7.0]


# ---------- backoff_delay -------------------------------------------------


def test_backoff_exponential_without_retry_after():
    # 1, 2, 4, 8, 16, 32, 60 (capped), 60, ...
    assert backoff_delay(0, None) == 1.0
    assert backoff_delay(1, None) == 2.0
    assert backoff_delay(4, None) == 16.0
    assert backoff_delay(5, None) == 32.0
    assert backoff_delay(6, None) == 60.0
    assert backoff_delay(10, None) == 60.0   # capped


def test_backoff_retry_after_seconds_wins_over_exponential():
    assert backoff_delay(0, "12") == 12.0
    assert backoff_delay(5, "3")  == 3.0


def test_backoff_ignores_unparseable_retry_after():
    # NHL CDN sends seconds. If it ever sent an HTTP-date or junk, fall
    # back to exponential rather than raising or trusting it.
    assert backoff_delay(2, "Mon, 01 Jan 2026 00:00:00 GMT") == 4.0
    assert backoff_delay(0, "not a number") == 1.0
    assert backoff_delay(0, "-5") == 1.0


# ---------- TokenBucket ---------------------------------------------------


class _FakeClock:
    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def test_token_bucket_burst_allows_n_no_wait():
    clock  = _FakeClock()
    sleeps: list[float] = []
    tb = TokenBucket(rate_per_sec=2.0, burst=5, _clock=clock)
    # First 5 calls should consume the burst capacity without sleeping.
    for _ in range(5):
        tb.wait(_sleep=sleeps.append)
    assert sleeps == []


def test_token_bucket_post_burst_sleeps_to_match_rate():
    clock  = _FakeClock()
    sleeps: list[float] = []

    def fake_sleep(dt):
        sleeps.append(dt)
        clock.advance(dt)

    tb = TokenBucket(rate_per_sec=2.0, burst=5, _clock=clock)
    for _ in range(5):
        tb.wait(_sleep=fake_sleep)
    # 6th call: bucket is empty, must wait 1/rate = 0.5s for one token.
    tb.wait(_sleep=fake_sleep)
    assert sleeps == [0.5]


def test_token_bucket_refills_with_elapsed_time():
    clock  = _FakeClock()
    sleeps: list[float] = []
    tb = TokenBucket(rate_per_sec=2.0, burst=5, _clock=clock)
    for _ in range(5):
        tb.wait(_sleep=sleeps.append)
    # 2 seconds pass while we do other work — bucket refills 4 tokens
    # (2/sec x 2s), capped at burst (5).
    clock.advance(2.0)
    for _ in range(4):
        tb.wait(_sleep=sleeps.append)
    # Still no sleeps because refill covered our 4 requests.
    assert sleeps == []
