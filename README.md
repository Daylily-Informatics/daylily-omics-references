# daylily-omics-references

Utilities for creating and validating the reference S3 buckets used by
[`daylily-ephemeral-cluster`](https://github.com/Daylily-Informatics/daylily-ephemeral-cluster).
The tooling in this repository was extracted from the legacy shell scripts that
used to live inside `daylily-ephemeral-cluster` and can now be versioned and
released independently.

The project exposes a Python package and a CLI entry point named
`daylily-omics-references`.  The CLI wraps the logic that was historically
implemented in `bin/create_daylily_omics_analysis_s3.sh` and the reference
validation that happened inside `bin/daylily-create-ephemeral-cluster`.

## Installation
Clone the tagged version of the repo you are looking for.

```bash
git clone https://github.com/Daylily-Informatics/daylily-omics-references.git
cd daylily-omics-references

```

The package targets Python 3.9 or newer.  Install it directly from the source
checkout:

```bash
python -m pip install .
```

or, during development, install an editable copy:

```bash
python -m pip install -e .
```

> The commands executed by the CLI shell out to the AWS CLI for recursive S3
> copy operations.  Ensure that the AWS CLI is installed and authenticated in
> the environment where you run these commands.

### Optional Conda environment

For contributors who prefer managing dependencies with Conda, this repository
ships an `environment.yml` file describing a minimal environment that matches
the package requirements.

```bash
conda env create -f environment.yml
conda activate daylily-omics-references
python -m pip install -e .
```

When `daylily-omics-references` is executed as part of
[`daylily-ephemeral-cluster`](https://github.com/Daylily-Informatics/daylily-ephemeral-cluster)
the Conda environment is provisioned automatically and installed into the
`DAY-EC` Conda environment on the cluster nodes.

## Usage

All commands honour the `--profile` option, allowing you to target the same AWS
profile that `daylily-ephemeral-cluster` relies on.  If you omit the
`--execute` flag the operations run in a safe dry-run mode.

```text
usage: daylily-omics-references [-h] [--profile PROFILE] [--region REGION]
                                [--log-level LOG_LEVEL]
                                {clone,verify,ensure} ...
```

### Clone a new reference bucket

```bash
daylily-omics-references \
    --profile daylily-service \
    clone \
    --bucket-prefix myorg \
    --region us-west-2 \
    --execute
```

The command will create the bucket `myorg-omics-analysis-us-west-2`, enable S3
transfer acceleration and copy the reference data for the default version
(`0.7.131c`).  Use `--exclude-hg38`, `--exclude-b37`, or `--exclude-giab` to
omit large subsets from the copy.  Pass `--use-acceleration` to copy via the
S3 accelerate endpoint.

### Verify an existing bucket

```bash
daylily-omics-references \
    --profile daylily-service \
    verify \
    --bucket myorg-omics-analysis-us-west-2
```

This validates that the bucket exists, contains the expected folder structure
and that its `s3_reference_data_version.info` marker matches the tagged version
packaged with this release.

### Ensure a bucket is ready for `daylily-ephemeral-cluster`

The `ensure` command combines the previous two flows.  It verifies the bucket if
it already exists; otherwise it creates it using the same cloning logic.

```bash
daylily-omics-references \
    --profile daylily-service \
    ensure \
    --bucket-prefix myorg \
    --region us-west-2 \
    --execute
```

When integrating with `daylily-ephemeral-cluster`, configure the cluster
creation scripts to call `daylily-omics-references ensure` using the tagged
release of this repository.  This guarantees that the S3 bucket backing a
cluster matches the expected reference version and is automatically created when
missing.

## Development

Run the test suite with `pytest`:

Install ``pytest`` and ``botocore`` before running the test suite:

```bash
python -m pip install -e .
python -m pip install pytest botocore
pytest
```

Pull requests should include unit tests and must pass the full test suite.
