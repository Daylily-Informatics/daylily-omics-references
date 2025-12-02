"""Simplified stand-ins for :mod:`boto3` sessions."""

from __future__ import annotations

import io
from types import SimpleNamespace
from typing import Any, Dict

from botocore.exceptions import ClientError
from botocore.response import StreamingBody


class S3Client:
    """Very small in-memory simulation of the S3 API used in tests."""

    def __init__(self, region_name: str | None = None) -> None:
        self.region_name = region_name or "us-east-1"
        self._buckets: Dict[str, Dict[str, bytes]] = {}
        self._bucket_policies: Dict[str, str] = {}
        self._active_stubber = None
        self.meta = SimpleNamespace(region_name=self.region_name)

    # ------------------------------------------------------------------
    def head_bucket(self, **params: Any) -> Dict[str, Any]:
        return self._dispatch("head_bucket", params)

    def create_bucket(self, **params: Any) -> Dict[str, Any]:
        return self._dispatch("create_bucket", params)

    def put_bucket_accelerate_configuration(self, **params: Any) -> Dict[str, Any]:
        return self._dispatch("put_bucket_accelerate_configuration", params)

    def put_bucket_policy(self, **params: Any) -> Dict[str, Any]:
        return self._dispatch("put_bucket_policy", params)

    def put_object(self, **params: Any) -> Dict[str, Any]:
        return self._dispatch("put_object", params)

    def get_object(self, **params: Any) -> Dict[str, Any]:
        return self._dispatch("get_object", params)

    def list_objects_v2(self, **params: Any) -> Dict[str, Any]:
        return self._dispatch("list_objects_v2", params)

    # ------------------------------------------------------------------
    def _dispatch(self, operation: str, params: Dict[str, Any]) -> Dict[str, Any]:
        stubber = getattr(self, "_active_stubber", None)
        if stubber is not None:
            return stubber.consume(operation, params)

        handler = getattr(self, f"_handle_{operation}", None)
        if handler is None:
            raise NotImplementedError(f"Operation {operation!r} is not implemented in the stub")
        return handler(**params)

    # ------------------------------------------------------------------
    def _handle_head_bucket(self, Bucket: str) -> Dict[str, Any]:
        if Bucket not in self._buckets:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")
        return {}

    def _handle_create_bucket(self, Bucket: str, **_: Any) -> Dict[str, Any]:
        if Bucket in self._buckets:
            raise ClientError(
                {"Error": {"Code": "BucketAlreadyOwnedByYou", "Message": "Bucket exists"}},
                "CreateBucket",
            )
        self._buckets[Bucket] = {}
        return {"Location": self.region_name}

    def _handle_put_bucket_accelerate_configuration(self, Bucket: str, **_: Any) -> Dict[str, Any]:
        if Bucket not in self._buckets:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "PutAccelerate")
        return {}

    def _handle_put_bucket_policy(self, Bucket: str, Policy: str) -> Dict[str, Any]:
        if Bucket not in self._buckets:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "PutBucketPolicy")
        self._bucket_policies[Bucket] = Policy
        return {}

    def _handle_put_object(self, Bucket: str, Key: str, Body: bytes | str) -> Dict[str, Any]:
        if Bucket not in self._buckets:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "PutObject")
        if isinstance(Body, str):
            data = Body.encode("utf-8")
        else:
            data = Body
        self._buckets[Bucket][Key] = bytes(data)
        return {"ETag": "stub"}

    def _handle_get_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        bucket = self._buckets.get(Bucket)
        if not bucket or Key not in bucket:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject")
        data = bucket[Key]
        return {"Body": StreamingBody(io.BytesIO(data), len(data))}

    def _handle_list_objects_v2(self, Bucket: str, Prefix: str, MaxKeys: int = 1) -> Dict[str, Any]:
        bucket = self._buckets.get(Bucket)
        if not bucket:
            return {}

        contents = []
        for key in bucket:
            if key.startswith(Prefix):
                contents.append({"Key": key})
                if len(contents) >= MaxKeys:
                    break
        if not contents:
            return {}
        return {"Contents": contents}


class _StsClient:
    def __init__(self, account_id: str):
        self._account_id = account_id

    def get_caller_identity(self) -> Dict[str, str]:
        return {"Account": self._account_id}


class Session:
    """A simplified analogue of :class:`boto3.session.Session`."""

    def __init__(
        self, profile_name: str | None = None, region_name: str | None = None, account_id: str = "000000000000"
    ) -> None:
        self.profile_name = profile_name
        self.region_name = region_name or "us-east-1"
        self._account_id = account_id
        self._clients: Dict[str, Any] = {}

    def client(self, service_name: str, *, region_name: str | None = None) -> Any:
        if service_name == "sts":
            return _StsClient(self._account_id)

        if service_name != "s3":
            raise ValueError(f"Unsupported service: {service_name}")

        client_region = region_name or self.region_name
        key = (service_name, client_region)
        if key not in self._clients:
            self._clients[key] = S3Client(client_region)
        return self._clients[key]


__all__ = ["Session", "S3Client"]
