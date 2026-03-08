from __future__ import annotations

from urllib.error import URLError

import pytest

import scripts.lib.pipeline_example_runner as runner
from scripts.lib.pipeline_example_runner import ApiClient


class _FakeHTTPResponse:
    def __init__(self, *, status: int, body: bytes, headers: dict[str, str] | None = None):
        self.status = status
        self._body = body
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeHTTPResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        _ = (exc_type, exc, tb)
        return False


def test_api_client_get_retries_on_socket_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def _fake_urlopen(req, timeout):
        _ = (req, timeout)
        calls["count"] += 1
        if calls["count"] < 3:
            raise OSError("connection reset")
        return _FakeHTTPResponse(status=200, body=b'{"ok": true}', headers={"content-type": "application/json"})

    monkeypatch.setattr(runner.request, "urlopen", _fake_urlopen)
    monkeypatch.setattr(runner.time, "sleep", lambda _: None)

    client = ApiClient(
        base_url="http://api.local/api/v1",
        token="test-token",
        timeout_seconds=3,
        get_retry_attempts=3,
        get_retry_backoff_seconds=0.0,
    )
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json_body == {"ok": True}
    assert calls["count"] == 3


def test_api_client_get_retry_exhaustion_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def _fake_urlopen(req, timeout):
        _ = (req, timeout)
        calls["count"] += 1
        raise URLError("temporary failure")

    monkeypatch.setattr(runner.request, "urlopen", _fake_urlopen)
    monkeypatch.setattr(runner.time, "sleep", lambda _: None)

    client = ApiClient(
        base_url="http://api.local/api/v1",
        token="test-token",
        timeout_seconds=3,
        get_retry_attempts=2,
        get_retry_backoff_seconds=0.0,
    )
    with pytest.raises(RuntimeError, match="Network error while calling GET"):
        client.get("/health")
    assert calls["count"] == 2


def test_api_client_post_does_not_retry_socket_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def _fake_urlopen(req, timeout):
        _ = (req, timeout)
        calls["count"] += 1
        raise OSError("connection reset")

    monkeypatch.setattr(runner.request, "urlopen", _fake_urlopen)

    client = ApiClient(
        base_url="http://api.local/api/v1",
        token="test-token",
        timeout_seconds=3,
        get_retry_attempts=5,
        get_retry_backoff_seconds=0.0,
    )
    with pytest.raises(RuntimeError, match="Socket error while calling POST"):
        client.post("/projects", json_payload={"name": "example"})
    assert calls["count"] == 1

