"""Utilities for creating and validating reference buckets."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Sequence

import boto3
from botocore.exceptions import ClientError

from .constants import (
    B37_PREFIXES,
    CORE_PREFIXES,
    DEFAULT_REFERENCE_VERSION,
    GIAB_PREFIXES,
    HG38_PREFIXES,
    SOURCE_BUCKET_BY_VERSION,
    VERSION_INFO_KEY,
)

_LOGGER = logging.getLogger(__name__)


class BucketVerificationError(RuntimeError):
    """Raised when a reference bucket fails verification."""

    def __init__(self, bucket: str, issues: Sequence[str]):
        message = f"Bucket '{bucket}' failed verification: {', '.join(issues)}"
        super().__init__(message)
        self.bucket = bucket
        self.issues = list(issues)


@dataclass
class CopyOperation:
    """Describes a copy operation from the source to the destination bucket."""

    description: str
    source_prefix: str
    destination_prefix: str
    include: bool = True


class ReferenceBucketManager:
    """Manager responsible for cloning and validating reference buckets."""

    def __init__(
        self,
        *,
        profile: str | None = None,
        region: str | None = None,
        session: boto3.session.Session | None = None,
        s3_client=None,
        command_runner: Callable[..., subprocess.CompletedProcess] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.profile = profile
        self.region = region
        self.session = session or boto3.session.Session(profile_name=profile, region_name=region)
        self.s3_client = s3_client or self.session.client("s3")
        self.command_runner = command_runner or subprocess.run
        self.logger = logger or _LOGGER

    # ------------------------------------------------------------------
    # Bucket helpers
    # ------------------------------------------------------------------
    def bucket_exists(self, bucket: str) -> bool:
        """Return ``True`` if *bucket* exists."""

        try:
            self.s3_client.head_bucket(Bucket=bucket)
        except ClientError:
            return False
        else:
            return True

    def create_bucket(self, bucket: str, region: str, *, dry_run: bool = False) -> None:
        """Create a bucket in *region* if ``dry_run`` is ``False``."""

        if dry_run:
            self.logger.info("[dry-run] Would create bucket %s in %s", bucket, region)
            return

        create_args = {"Bucket": bucket}
        if region != "us-east-1":
            create_args["CreateBucketConfiguration"] = {"LocationConstraint": region}

        self.logger.info("Creating bucket %s in %s", bucket, region)
        self.s3_client.create_bucket(**create_args)

        # Accelerate access is always enabled to match the historic behaviour of
        # the shell script this manager supersedes.
        self.logger.debug("Enabling transfer acceleration for bucket %s", bucket)
        self.s3_client.put_bucket_accelerate_configuration(
            Bucket=bucket, AccelerateConfiguration={"Status": "Enabled"}
        )

    # ------------------------------------------------------------------
    # Version helpers
    # ------------------------------------------------------------------
    def write_version_file(self, bucket: str, version: str, *, dry_run: bool = False) -> None:
        """Write the version marker file to *bucket*."""

        if dry_run:
            self.logger.info(
                "[dry-run] Would upload %s with version %s", VERSION_INFO_KEY, version
            )
            return

        self.logger.debug("Uploading version marker %s to %s", VERSION_INFO_KEY, bucket)
        self.s3_client.put_object(
            Bucket=bucket,
            Key=VERSION_INFO_KEY,
            Body=version.encode("utf-8"),
        )

    def read_bucket_version(self, bucket: str) -> str | None:
        """Return the version recorded in the bucket, if present."""

        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=VERSION_INFO_KEY)
        except ClientError:
            return None

        body = response.get("Body")
        if body is None:
            return None
        data = body.read().decode("utf-8").strip()
        return data

    # ------------------------------------------------------------------
    # Copy helpers
    # ------------------------------------------------------------------
    def _build_copy_plan(
        self,
        *,
        include_hg38: bool,
        include_b37: bool,
        include_giab: bool,
    ) -> List[CopyOperation]:
        plan: List[CopyOperation] = []

        for prefix in CORE_PREFIXES:
            plan.append(
                CopyOperation(
                    description=prefix.rstrip("/"),
                    source_prefix=prefix,
                    destination_prefix=prefix,
                )
            )

        if include_hg38:
            for prefix in HG38_PREFIXES:
                plan.append(
                    CopyOperation(
                        description=prefix.rstrip("/"),
                        source_prefix=prefix,
                        destination_prefix=prefix,
                    )
                )

        if include_b37:
            for prefix in B37_PREFIXES:
                plan.append(
                    CopyOperation(
                        description=prefix.rstrip("/"),
                        source_prefix=prefix,
                        destination_prefix=prefix,
                    )
                )

        if include_giab:
            for prefix in GIAB_PREFIXES:
                plan.append(
                    CopyOperation(
                        description=prefix.rstrip("/"),
                        source_prefix=prefix,
                        destination_prefix=prefix,
                    )
                )

        return plan

    def _run_copy_command(
        self,
        *,
        source_bucket: str,
        destination_bucket: str,
        prefix: str,
        dry_run: bool,
        use_acceleration: bool,
        log_file: Path | None,
        request_payer: str = "requester",
    ) -> None:
        command = [
            "aws",
            "s3",
            "cp",
            f"s3://{source_bucket}/{prefix}",
            f"s3://{destination_bucket}/{prefix}",
            "--recursive",
            "--request-payer",
            request_payer,
            "--metadata-directive",
            "REPLACE",
        ]

        if use_acceleration:
            command.extend(["--endpoint-url", "https://s3-accelerate.amazonaws.com"])

        env = os.environ.copy()
        if self.profile:
            env["AWS_PROFILE"] = self.profile

        if dry_run:
            self.logger.info("[dry-run] %s", " ".join(shlex.quote(part) for part in command))
            return

        if log_file:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            with log_file.open("a", encoding="utf-8") as handle:
                handle.write(f"$ {' '.join(shlex.quote(part) for part in command)}\n")

        self.logger.debug("Running command: %s", " ".join(command))
        result = self.command_runner(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )

        if log_file:
            with log_file.open("a", encoding="utf-8") as handle:
                if result.stdout:
                    handle.write(result.stdout)
                if result.stderr:
                    handle.write(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"aws s3 cp for prefix {prefix!r} failed with code {result.returncode}: {result.stderr}"
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def clone_reference_bucket(
        self,
        *,
        bucket_prefix: str,
        region: str,
        version: str = DEFAULT_REFERENCE_VERSION,
        dry_run: bool = True,
        include_hg38: bool = True,
        include_b37: bool = True,
        include_giab: bool = True,
        use_acceleration: bool = False,
        log_file: str | None = None,
    ) -> str:
        """Clone reference data into a new bucket and return the bucket name."""

        if version not in SOURCE_BUCKET_BY_VERSION:
            raise ValueError(f"Unsupported reference version: {version}")

        source_bucket = SOURCE_BUCKET_BY_VERSION[version]
        bucket_name = f"{bucket_prefix}-omics-analysis-{region}"
        log_path = Path(log_file) if log_file else None

        if self.bucket_exists(bucket_name):
            raise ValueError(f"Bucket '{bucket_name}' already exists")

        # Create bucket (and optionally enable acceleration)
        self.create_bucket(bucket_name, region, dry_run=dry_run)

        plan = self._build_copy_plan(
            include_hg38=include_hg38,
            include_b37=include_b37,
            include_giab=include_giab,
        )

        if dry_run:
            self.logger.info(
                "[dry-run] Would copy version marker %s to %s", VERSION_INFO_KEY, bucket_name
            )
        else:
            self.write_version_file(bucket_name, version, dry_run=False)

        total_ops = len(plan)
        for index, operation in enumerate(plan, start=1):
            if not operation.include:
                continue
            self.logger.info(
                "Copying %s (%d/%d)", operation.description, index, total_ops
            )
            self._run_copy_command(
                source_bucket=source_bucket,
                destination_bucket=bucket_name,
                prefix=operation.source_prefix,
                dry_run=dry_run,
                use_acceleration=use_acceleration,
                log_file=log_path,
            )

        self.logger.info("Bucket %s is ready", bucket_name)
        return bucket_name

    def verify_bucket(
        self,
        bucket: str,
        *,
        expected_version: str = DEFAULT_REFERENCE_VERSION,
        include_hg38: bool = True,
        include_b37: bool = True,
        include_giab: bool = True,
    ) -> None:
        """Verify that *bucket* contains the expected structure and version."""

        if expected_version not in SOURCE_BUCKET_BY_VERSION:
            raise ValueError(f"Unsupported reference version: {expected_version}")

        if not self.bucket_exists(bucket):
            raise BucketVerificationError(bucket, [f"bucket ({bucket}) does not exist"])

        issues: List[str] = []
        bucket_version = self.read_bucket_version(bucket)
        if bucket_version is None:
            issues.append("missing version marker")
        elif bucket_version != expected_version:
            issues.append(
                f"version mismatch (expected {expected_version}, found {bucket_version})"
            )

        prefixes_to_check: List[str] = list(CORE_PREFIXES)
        if include_hg38:
            prefixes_to_check.extend(HG38_PREFIXES)
        if include_b37:
            prefixes_to_check.extend(B37_PREFIXES)
        if include_giab:
            prefixes_to_check.extend(GIAB_PREFIXES)

        for prefix in prefixes_to_check:
            if not self._prefix_exists(bucket, prefix):
                issues.append(f"missing objects under {prefix}")

        if issues:
            raise BucketVerificationError(bucket, issues)

        self.logger.info(
            "Bucket %s passed verification for version %s", bucket, expected_version
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _prefix_exists(self, bucket: str, prefix: str) -> bool:
        response = self.s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
        return "Contents" in response and bool(response["Contents"])

    # ------------------------------------------------------------------
    def ensure_bucket(
        self,
        *,
        bucket_prefix: str,
        region: str,
        version: str = DEFAULT_REFERENCE_VERSION,
        include_hg38: bool = True,
        include_b37: bool = True,
        include_giab: bool = True,
        use_acceleration: bool = False,
        log_file: str | None = None,
        dry_run: bool = False,
        create_missing: bool = True,
    ) -> str:
        """Ensure a bucket exists and matches the expected structure."""

        bucket_name = f"{bucket_prefix}-omics-analysis-{region}"
        if self.bucket_exists(bucket_name):
            self.logger.debug("Bucket %s already exists, verifying", bucket_name)
            self.verify_bucket(
                bucket_name,
                expected_version=version,
                include_hg38=include_hg38,
                include_b37=include_b37,
                include_giab=include_giab,
            )
            return bucket_name

        if not create_missing:
            raise BucketVerificationError(bucket_name, ["bucket is missing"])

        self.logger.info(
            "Bucket %s is missing; cloning reference data (dry_run=%s)", bucket_name, dry_run
        )
        return self.clone_reference_bucket(
            bucket_prefix=bucket_prefix,
            region=region,
            version=version,
            dry_run=dry_run,
            include_hg38=include_hg38,
            include_b37=include_b37,
            include_giab=include_giab,
            use_acceleration=use_acceleration,
            log_file=log_file,
        )
