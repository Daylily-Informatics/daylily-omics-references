from __future__ import annotations

import io
import json
import subprocess
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


def test_s3_client_lists_without_prefix():
    client = boto3.session.S3Client()

    client.create_bucket(Bucket="target")
    client.put_object(Bucket="target", Key="example/key", Body=b"data")

    response = client.list_objects_v2(Bucket="target")

    assert response == {"Contents": [{"Key": "example/key"}]}


def test_run_copy_command_sets_region_environment_and_profile():
    captured_env = {}
    captured_command = []

    def runner(command, *, check, capture_output, text, env):
        captured_env.update(env)
        captured_command.extend(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    manager = ReferenceBucketManager(
        region="us-west-2", profile="dev-profile", command_runner=runner
    )

    manager._run_copy_command(
        source_bucket="source",
        destination_bucket="dest",
        prefix="foo/",
        dry_run=False,
        use_acceleration=False,
        log_file=None,
    )

    assert captured_env["AWS_REGION"] == "us-west-2"
    assert captured_env["AWS_DEFAULT_REGION"] == "us-west-2"
    assert captured_env["AWS_PROFILE"] == "dev-profile"
    assert captured_command[-2:] == ["--profile", "dev-profile"]


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


def test_clone_reference_bucket_dry_run_excludes_data_lib_prefix():
    manager = ReferenceBucketManager()

    with mock.patch.object(manager, "bucket_exists", return_value=False), \
        mock.patch.object(manager, "create_bucket"), \
        mock.patch.object(manager, "write_version_file"), \
        mock.patch.object(manager, "_run_copy_command") as mock_copy:
        manager.clone_reference_bucket(
            bucket_prefix="test",
            region="us-west-2",
            dry_run=True,
            include_hg38=False,
            include_b37=False,
            include_giab=False,
        )

    copied_prefixes = [call.kwargs["prefix"] for call in mock_copy.call_args_list]
    assert copied_prefixes == list(CORE_PREFIXES)
    assert "data/lib/" not in copied_prefixes


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


def test_verify_bucket_excludes_data_lib_prefix():
    manager = ReferenceBucketManager()

    with mock.patch.object(manager, "bucket_exists", return_value=True), \
        mock.patch.object(manager, "read_bucket_version", return_value=DEFAULT_REFERENCE_VERSION), \
        mock.patch.object(manager, "_prefix_exists", return_value=True) as mock_prefix_exists:
        manager.verify_bucket(
            "target",
            include_hg38=False,
            include_b37=False,
            include_giab=False,
        )

    checked_prefixes = [call.args[1] for call in mock_prefix_exists.call_args_list]
    assert checked_prefixes == list(CORE_PREFIXES)
    assert "data/lib/" not in checked_prefixes


def test_core_prefixes_do_not_include_data_lib():
    assert "data/lib/" not in CORE_PREFIXES


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
    waiter = mock.Mock()
    waiter.wait = mock.Mock()
    client.get_waiter.return_value = waiter
    return client


def test_create_bucket_applies_policy():
    s3 = _mock_s3_client("us-west-2")
    sts = mock.Mock()
    sts.get_caller_identity.return_value = {"Account": "123456789012"}

    manager = ReferenceBucketManager(s3_client=s3, sts_client=sts)

    manager.create_bucket("target", "us-west-2", dry_run=False)

    s3.create_bucket.assert_called_once_with(
        Bucket="target",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )
    s3.get_waiter.assert_called_once_with("bucket_exists")
    s3.get_waiter.return_value.wait.assert_called_once_with(Bucket="target")
    s3.put_bucket_accelerate_configuration.assert_called_once_with(
        Bucket="target", AccelerateConfiguration={"Status": "Enabled"}
    )

    s3.put_bucket_policy.assert_called_once()
    policy = json.loads(s3.put_bucket_policy.call_args.kwargs["Policy"])
    assert {"AWS": "arn:aws:iam::123456789012:root"} in [
        statement.get("Principal") for statement in policy["Statement"]
    ]


def test_create_bucket_dry_run_skips_api_calls():
    s3 = _mock_s3_client("us-west-2")
    sts = mock.Mock()

    manager = ReferenceBucketManager(s3_client=s3, sts_client=sts)

    manager.create_bucket("target", "us-west-2", dry_run=True)

    s3.create_bucket.assert_not_called()
    s3.get_waiter.assert_not_called()
    s3.put_bucket_accelerate_configuration.assert_not_called()
    s3.put_bucket_policy.assert_not_called()


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


def test_wait_for_bucket_listable_succeeds():
    session = mock.Mock()
    client = _mock_s3_client("us-west-2")
    client.list_objects_v2.return_value = {}

    manager = ReferenceBucketManager(session=session, s3_client=client)

    manager._wait_for_bucket_listable("target", attempts=2, delay_seconds=0)

    client.list_objects_v2.assert_called_once_with(Bucket="target", MaxKeys=1)


def test_wait_for_bucket_listable_retries_and_raises(monkeypatch):
    session = mock.Mock()
    client = _mock_s3_client("us-west-2")
    error = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "Not ready"}},
        "ListObjectsV2",
    )
    client.list_objects_v2.side_effect = error
    monkeypatch.setattr("time.sleep", lambda seconds: None)

    manager = ReferenceBucketManager(session=session, s3_client=client)

    with pytest.raises(RuntimeError):
        manager._wait_for_bucket_listable("target", attempts=2, delay_seconds=0)

    assert client.list_objects_v2.call_count == 2


def test_clone_reference_bucket_waits_for_listable():
    manager = ReferenceBucketManager()

    with mock.patch.object(manager, "bucket_exists", return_value=False), \
        mock.patch.object(manager, "create_bucket") as mock_create, \
        mock.patch.object(manager, "_wait_for_bucket_listable") as mock_wait, \
        mock.patch.object(manager, "write_version_file") as mock_write, \
        mock.patch.object(manager, "_run_copy_command") as mock_copy:
        bucket = manager.clone_reference_bucket(
            bucket_prefix="test",
            region="us-west-2",
            dry_run=False,
            include_hg38=False,
            include_b37=False,
            include_giab=False,
        )

    assert bucket == "test-omics-analysis-us-west-2"
    mock_create.assert_called_once_with(bucket, "us-west-2", dry_run=False)
    mock_wait.assert_called_once_with(bucket)
    mock_write.assert_called_once_with(bucket, DEFAULT_REFERENCE_VERSION, dry_run=False)
    assert mock_copy.call_count == len(CORE_PREFIXES)
