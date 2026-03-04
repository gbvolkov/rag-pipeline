from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path

import boto3

from app.core.config import get_settings


class BlobStore:
    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> str:
        raise NotImplementedError

    def get_bytes(self, uri: str) -> bytes:
        raise NotImplementedError


@dataclass
class FilesystemBlobStore(BlobStore):
    root: Path

    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> str:
        path = (self.root / key).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return f"fs://{path.as_posix()}"

    def get_bytes(self, uri: str) -> bytes:
        path = Path(uri.removeprefix("fs://"))
        return path.read_bytes()


@dataclass
class MinioBlobStore(BlobStore):
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool = False

    def __post_init__(self) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=f"http{'s' if self.secure else ''}://{self.endpoint}",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except Exception:
            self._client.create_bucket(Bucket=self.bucket)

    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> str:
        self._client.upload_fileobj(
            Fileobj=io.BytesIO(payload),
            Bucket=self.bucket,
            Key=key,
            ExtraArgs={"ContentType": content_type},
        )
        return f"s3://{self.bucket}/{key}"

    def get_bytes(self, uri: str) -> bytes:
        raw = uri.removeprefix("s3://")
        bucket, _, key = raw.partition("/")
        buffer = io.BytesIO()
        self._client.download_fileobj(Bucket=bucket, Key=key, Fileobj=buffer)
        return buffer.getvalue()


def build_blob_store() -> BlobStore:
    settings = get_settings()
    if settings.blob_backend == "minio":
        return MinioBlobStore(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            bucket=settings.minio_bucket,
            secure=settings.minio_secure,
        )
    settings.local_blob_root.mkdir(parents=True, exist_ok=True)
    return FilesystemBlobStore(root=settings.local_blob_root)

