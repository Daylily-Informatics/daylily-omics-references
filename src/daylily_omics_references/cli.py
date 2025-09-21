"""Command line interface for the reference bucket manager."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Iterable

from .constants import DEFAULT_REFERENCE_VERSION, SUPPORTED_REFERENCE_VERSIONS
from .manager import BucketVerificationError, ReferenceBucketManager


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(levelname)s: %(message)s",
    )


def _parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage Daylily omics analysis reference buckets",
    )
    parser.add_argument(
        "--profile",
        help="AWS profile to use for boto3 and AWS CLI calls",
    )
    parser.add_argument(
        "--region",
        help="Default AWS region to target (may be overridden per command)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    clone = sub.add_parser("clone", help="Clone the reference bucket into a new bucket")
    clone.add_argument("--bucket-prefix", required=True, help="Prefix for the new bucket")
    clone.add_argument("--region", help="AWS region for the new bucket")
    clone.add_argument(
        "--version",
        default=DEFAULT_REFERENCE_VERSION,
        choices=SUPPORTED_REFERENCE_VERSIONS,
        help="Reference data version to clone",
    )
    clone.add_argument(
        "--execute",
        action="store_true",
        help="Execute the copy instead of performing a dry-run",
    )
    clone.add_argument(
        "--exclude-hg38",
        action="store_true",
        help="Exclude hg38 references and annotations",
    )
    clone.add_argument(
        "--exclude-b37",
        action="store_true",
        help="Exclude b37 references and annotations",
    )
    clone.add_argument(
        "--exclude-giab",
        action="store_true",
        help="Exclude GIAB concordance reads",
    )
    clone.add_argument(
        "--use-acceleration",
        action="store_true",
        help="Use the S3 accelerate endpoint during copy operations",
    )
    clone.add_argument(
        "--log-file",
        help="Optional path to capture AWS CLI output",
    )

    verify = sub.add_parser("verify", help="Verify that a reference bucket matches expectations")
    verify.add_argument("--bucket", required=True, help="Name of the bucket to verify")
    verify.add_argument(
        "--version",
        default=DEFAULT_REFERENCE_VERSION,
        choices=SUPPORTED_REFERENCE_VERSIONS,
        help="Expected reference data version",
    )
    verify.add_argument(
        "--exclude-hg38",
        action="store_true",
        help="Skip checking hg38 references",
    )
    verify.add_argument(
        "--exclude-b37",
        action="store_true",
        help="Skip checking b37 references",
    )
    verify.add_argument(
        "--exclude-giab",
        action="store_true",
        help="Skip checking GIAB reads",
    )

    ensure = sub.add_parser(
        "ensure",
        help=(
            "Verify a bucket exists and matches expectations, creating it from the "
            "reference repository if missing"
        ),
    )
    ensure.add_argument("--bucket-prefix", required=True, help="Prefix for the target bucket")
    ensure.add_argument("--region", help="Region that contains the bucket")
    ensure.add_argument(
        "--version",
        default=DEFAULT_REFERENCE_VERSION,
        choices=SUPPORTED_REFERENCE_VERSIONS,
        help="Expected reference data version",
    )
    ensure.add_argument(
        "--execute",
        action="store_true",
        help="Create the bucket if missing (otherwise a dry-run is performed)",
    )
    ensure.add_argument(
        "--no-create",
        action="store_true",
        help="Fail if the bucket is missing instead of creating it",
    )
    ensure.add_argument(
        "--exclude-hg38",
        action="store_true",
        help="Skip cloning and verification of hg38 references",
    )
    ensure.add_argument(
        "--exclude-b37",
        action="store_true",
        help="Skip cloning and verification of b37 references",
    )
    ensure.add_argument(
        "--exclude-giab",
        action="store_true",
        help="Skip cloning and verification of GIAB reads",
    )
    ensure.add_argument(
        "--use-acceleration",
        action="store_true",
        help="Use the S3 accelerate endpoint during clone operations",
    )
    ensure.add_argument(
        "--log-file",
        help="Optional path to capture AWS CLI output when cloning",
    )

    return parser.parse_args(list(argv))


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    _setup_logging(args.log_level)

    manager = ReferenceBucketManager(profile=args.profile, region=args.region)

    include_hg38 = not getattr(args, "exclude_hg38", False)
    include_b37 = not getattr(args, "exclude_b37", False)
    include_giab = not getattr(args, "exclude_giab", False)

    try:
        if args.command == "clone":
            region = args.region or manager.region
            if not region:
                raise SystemExit("--region must be specified globally or per command")
            manager.clone_reference_bucket(
                bucket_prefix=args.bucket_prefix,
                region=region,
                version=args.version,
                dry_run=not args.execute,
                include_hg38=include_hg38,
                include_b37=include_b37,
                include_giab=include_giab,
                use_acceleration=args.use_acceleration,
                log_file=args.log_file,
            )
        elif args.command == "verify":
            manager.verify_bucket(
                args.bucket,
                expected_version=args.version,
                include_hg38=include_hg38,
                include_b37=include_b37,
                include_giab=include_giab,
            )
        elif args.command == "ensure":
            region = args.region or manager.region
            if not region:
                raise SystemExit("--region must be specified globally or per command")
            manager.ensure_bucket(
                bucket_prefix=args.bucket_prefix,
                region=region,
                version=args.version,
                include_hg38=include_hg38,
                include_b37=include_b37,
                include_giab=include_giab,
                use_acceleration=args.use_acceleration,
                log_file=args.log_file,
                dry_run=not args.execute,
                create_missing=not args.no_create,
            )
        else:  # pragma: no cover - defensive fallback
            raise SystemExit(f"Unknown command: {args.command}")
    except BucketVerificationError as exc:  # pragma: no cover - exercised in tests
        logging.error(str(exc))
        return 2

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
