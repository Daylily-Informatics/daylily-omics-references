from __future__ import annotations

import io
from unittest import mock

import boto3
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber
from botocore.exceptions import ClientError

from daylily_omics_references import BucketVerificationError, ReferenceBucketManager
from daylily_omics_references.constants import (
    B37_PREFIXES,
    CORE_PREFIXES,
    DEFAULT_REFERENCE_VERSION,
    GIAB_PREFIXES,
    HG38_PREFIXES,
    VERSION_INFO_KEY,
)


def _version_body(version: str) -> StreamingBody:
    data = version.encode("utf-8")
    return StreamingBody(io.BytesIO(data), len(data))


def test_clone_reference_bucket_dry_run():
    manager = ReferenceBucketManager()

    with mock.patch.object(manager, "bucket_exists", return_value=False), \
        mock.patch.object(manager, "create_bucket") as mock_create, \
        mock.patch.object(manager, "write_version_file") as mock_write, \
        mock.patch.object(manager, "_run_copy_command") as mock_copy:
        bucket = manager.clone_reference_bucket(
            bucket_prefix="test",
            region="us-west-2",
            dry_run=True,
        )

    assert bucket == "test-omics-analysis-us-west-2"
    mock_create.assert_called_once_with(bucket, "us-west-2", dry_run=True)
    mock_write.assert_not_called()
    expected_calls = len(CORE_PREFIXES) + len(HG38_PREFIXES) + len(B37_PREFIXES) + len(GIAB_PREFIXES)
    assert mock_copy.call_count == expected_calls


@pytest.mark.parametrize(
    "include_hg38,include_b37,include_giab",
    [
        (True, True, True),
        (False, False, False),
    ],
)
def test_verify_bucket_success(include_hg38: bool, include_b37: bool, include_giab: bool):
    session = boto3.session.Session(region_name="us-west-2")
    client = session.client("s3")
    manager = ReferenceBucketManager(session=session, s3_client=client)
    stubber = Stubber(client)

    prefixes = list(CORE_PREFIXES)
    if include_hg38:
        prefixes.extend(HG38_PREFIXES)
    if include_b37:
        prefixes.extend(B37_PREFIXES)
    if include_giab:
        prefixes.extend(GIAB_PREFIXES)

    with stubber:
        stubber.add_response("head_bucket", {}, {"Bucket": "target"})
        stubber.add_response(
            "get_object",
            {"Body": _version_body(DEFAULT_REFERENCE_VERSION)},
            {"Bucket": "target", "Key": VERSION_INFO_KEY},
        )
        for prefix in prefixes:
            stubber.add_response(
                "list_objects_v2",
                {"Contents": [{"Key": f"{prefix}dummy"}]},
                {"Bucket": "target", "Prefix": prefix, "MaxKeys": 1},
            )

        manager.verify_bucket(
            "target",
            include_hg38=include_hg38,
            include_b37=include_b37,
            include_giab=include_giab,
        )


def test_verify_bucket_missing_prefix():
    session = boto3.session.Session(region_name="us-west-2")
    client = session.client("s3")
    manager = ReferenceBucketManager(session=session, s3_client=client)
    stubber = Stubber(client)

    prefixes = list(CORE_PREFIXES) + list(HG38_PREFIXES) + list(B37_PREFIXES) + list(GIAB_PREFIXES)

    with stubber:
        stubber.add_response("head_bucket", {}, {"Bucket": "target"})
        stubber.add_response(
            "get_object",
            {"Body": _version_body(DEFAULT_REFERENCE_VERSION)},
            {"Bucket": "target", "Key": VERSION_INFO_KEY},
        )

        first = True
        for prefix in prefixes:
            if first:
                stubber.add_response(
                    "list_objects_v2",
                    {},
                    {"Bucket": "target", "Prefix": prefix, "MaxKeys": 1},
                )
                first = False
            else:
                stubber.add_response(
                    "list_objects_v2",
                    {"Contents": [{"Key": f"{prefix}dummy"}]},
                    {"Bucket": "target", "Prefix": prefix, "MaxKeys": 1},
                )

        with pytest.raises(BucketVerificationError) as exc:
            manager.verify_bucket("target")

    assert "missing objects" in str(exc.value)


def test_ensure_bucket_missing_without_create():
    manager = ReferenceBucketManager()
    with mock.patch.object(manager, "bucket_exists", return_value=False):
        with pytest.raises(BucketVerificationError):
            manager.ensure_bucket(
                bucket_prefix="test",
                region="us-west-2",
                create_missing=False,
            )


def _mock_s3_client(region: str) -> mock.Mock:
    client = mock.Mock()
    client.meta = mock.Mock()
    client.meta.region_name = region
    return client


def _permanent_redirect_error(region: str) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "301", "Message": "Moved Permanently"},
            "ResponseMetadata": {"HTTPHeaders": {"x-amz-bucket-region": region}},
        },
        "HeadBucket",
    )


def test_bucket_exists_redirects_to_bucket_region():
    session = mock.Mock()
    first = _mock_s3_client("us-east-1")
    second = _mock_s3_client("us-west-2")
    session.client.side_effect = [second]

    manager = ReferenceBucketManager(session=session, s3_client=first)
    first.head_bucket.side_effect = _permanent_redirect_error("us-west-2")
    second.head_bucket.return_value = {}

    assert manager.bucket_exists("target")
    session.client.assert_called_once_with("s3", region_name="us-west-2")
    assert manager.s3_client is second
    assert manager.region == "us-west-2"


def test_verify_bucket_handles_redirect(monkeypatch):
    session = mock.Mock()
    first = _mock_s3_client("us-east-1")
    second = _mock_s3_client("us-west-2")
    session.client.side_effect = [second]

    manager = ReferenceBucketManager(session=session, s3_client=first)
    first.head_bucket.side_effect = _permanent_redirect_error("us-west-2")

    second.head_bucket.return_value = {}
    second.get_object.return_value = {"Body": _version_body(DEFAULT_REFERENCE_VERSION)}

    def _list_objects_side_effect(**kwargs):
        return {"Contents": [{"Key": f"{kwargs['Prefix']}dummy"}]}

    second.list_objects_v2.side_effect = _list_objects_side_effect

    manager.verify_bucket("target")

    session.client.assert_called_once_with("s3", region_name="us-west-2")
    assert manager.s3_client is second
    assert manager.region == "us-west-2"
