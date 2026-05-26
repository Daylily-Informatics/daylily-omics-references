# daylily-omics-references

Bash utilities for creating and validating the reference S3 buckets used by
[`daylily-ephemeral-cluster`](https://github.com/Daylily-Informatics/daylily-ephemeral-cluster).
The project now ships a single shell script that wraps AWS CLI commands to clone
public reference data, validate an existing bucket, or ensure a bucket exists
and matches the expected structure.

> The commands shell out to the AWS CLI for recursive S3 copy operations. Ensure
> that the AWS CLI and `jq` are installed and that your environment is
> authenticated before running the script.

## Usage

The entry point lives at `scripts/daylily-omics-references.sh`. Global options
may be placed before the subcommand.

```
Usage: daylily-omics-references.sh [GLOBAL OPTIONS] <command> [ARGS]

Global options:
  --profile PROFILE   AWS profile to use
  --region REGION     AWS region to target (required for clone/ensure)

Commands:
  clone   Create a new reference bucket from the public source
  verify  Validate that a bucket matches the expected structure
  ensure  Verify a bucket or create it if missing
```

### Clone a new reference bucket

```
scripts/daylily-omics-references.sh \
  --profile daylily-service \
  clone \
  --bucket-prefix myorg \
  --region us-west-2 \
  --execute
```

This creates `myorg-omics-analysis-us-west-2`, enables transfer acceleration
and copies the default reference version (`0.7.131c`). Use `--exclude-hg38`,
`--exclude-b37`, or `--exclude-giab` to omit large subsets. Pass
`--use-acceleration` to copy via the S3 accelerate endpoint.

### Verify an existing bucket

```
scripts/daylily-omics-references.sh \
  --profile daylily-service \
  verify \
  --bucket myorg-omics-analysis-us-west-2
```

This validates that the bucket exists, contains the expected folder structure,
has the exact DAY-EC headnode readiness objects that mount under `/fsx/data`,
and that its `s3_reference_data_version.info` marker matches the default version.

### Ensure a bucket is ready for `daylily-ephemeral-cluster`

```
scripts/daylily-omics-references.sh \
  --profile daylily-service \
  ensure \
  --bucket-prefix myorg \
  --region us-west-2 \
  --execute
```

The command verifies the bucket when it already exists; otherwise it creates the
bucket using the same cloning logic.

## Development

The repository now consists solely of the bash script above. No Python
dependencies are required. If you modify the script, ensure it remains POSIX
shell compatible and keep the usage examples up to date.
