from __future__ import annotations

from botocore.exceptions import ClientError

from app.services import blobstore


def _client_error(code: str, status: int) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "HeadBucket",
    )


def test_minio_blobstore_creates_bucket_only_when_missing(monkeypatch):
    calls: list[tuple[str, str]] = []

    class _FakeClient:
        def head_bucket(self, Bucket: str):
            calls.append(("head_bucket", Bucket))
            raise _client_error("404", 404)

        def create_bucket(self, Bucket: str):
            calls.append(("create_bucket", Bucket))

    monkeypatch.setattr(blobstore.boto3, "client", lambda *args, **kwargs: _FakeClient())

    blobstore.MinioBlobStore(
        endpoint="localhost:9000",
        access_key="minio",
        secret_key="minio123",
        bucket="artifacts",
    )

    assert calls == [("head_bucket", "artifacts"), ("create_bucket", "artifacts")]


def test_minio_blobstore_propagates_non_not_found_head_bucket_errors(monkeypatch):
    calls: list[tuple[str, str]] = []

    class _FakeClient:
        def head_bucket(self, Bucket: str):
            calls.append(("head_bucket", Bucket))
            raise _client_error("403", 403)

        def create_bucket(self, Bucket: str):
            calls.append(("create_bucket", Bucket))

    monkeypatch.setattr(blobstore.boto3, "client", lambda *args, **kwargs: _FakeClient())

    try:
        blobstore.MinioBlobStore(
            endpoint="localhost:9000",
            access_key="minio",
            secret_key="minio123",
            bucket="artifacts",
        )
    except ClientError as exc:
        assert exc.response["Error"]["Code"] == "403"
    else:
        raise AssertionError("Expected head_bucket authorization error to propagate")

    assert calls == [("head_bucket", "artifacts")]
