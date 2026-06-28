"""Tests for bronze.tracking_ingest.fetch_tracking — the HTTP + parse-and-
validate step.

The load-bearing invariant here: a 200 response with a body that ISN'T a
non-empty top-level JSON list must NOT be reported as success. Otherwise
the job would PUT garbage (Cloudflare interstitials, error HTML pages,
empty arrays) to durable bronze storage, and downstream silver would
either fail to parse it or silently produce empty frame sets.

requests.get is monkeypatched per-test rather than mocked via the network
— faster, no external dependency, and forces each test to declare exactly
what HTTP shape it's exercising.
"""

from types import SimpleNamespace

import pytest
import requests

import tracking_ingest
from tracking_ingest import fetch_tracking


def _resp(status_code, content=b"", text=""):
    """Minimal stand-in for a requests.Response."""
    return SimpleNamespace(status_code=status_code, content=content, text=text)


@pytest.fixture
def patch_get(monkeypatch):
    """Returns a function that installs a fake requests.get returning the
    given response (or raising the given exception)."""
    def _patch(resp_or_exc):
        def fake_get(*args, **kwargs):
            if isinstance(resp_or_exc, Exception):
                raise resp_or_exc
            return resp_or_exc
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
