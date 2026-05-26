"""Constants shared across the project."""

from __future__ import annotations

DEFAULT_REFERENCE_VERSION = "0.7.131c"

# All supported reference versions and their corresponding source buckets.
SOURCE_BUCKET_BY_VERSION = {
    DEFAULT_REFERENCE_VERSION: "daylily-omics-analysis-references-public",
}

SUPPORTED_REFERENCE_VERSIONS = tuple(SOURCE_BUCKET_BY_VERSION.keys())

# Prefixes that are always required in a destination bucket.
CORE_PREFIXES = (
    "runtime_assets/cluster_boot_config/",
    "runtime_assets/cached_envs/",
    "runtime_assets/tool_specific_resources/",
    "runtime_assets/budget_tags/",
)

# Exact assets DAY-EC must see under /fsx/references/runtime_assets before workflow launch.
DAYEC_REQUIRED_OBJECT_KEYS = (
    "runtime_assets/cached_envs/apptainer_1.4.5_amd64.deb",
    "runtime_assets/tool_specific_resources/cromwell_87.jar",
    "runtime_assets/tool_specific_resources/womtool_87.jar",
)

DAYEC_REQUIRED_PREFIXES = (
    "runtime_assets/cached_envs/conda/",
)

# Optional prefixes that may be toggled via CLI flags.
HG38_PREFIXES = (
    "genomic_data/organism_references/H_sapiens/hg38/",
    "genomic_data/organism_annotations/H_sapiens/hg38/",
)

B37_PREFIXES = (
    "genomic_data/organism_references/H_sapiens/b37/",
    "genomic_data/organism_annotations/H_sapiens/b37/",
)

GIAB_PREFIXES = (
    "genomic_data/organism_reads_slim/",
)

PUBLIC_SAFE_SCAN_PREFIXES = (
    "runtime_assets/",
)

PUBLIC_FORBIDDEN_KEY_FRAGMENTS = (
    ".lic",
    "license",
    "sentieon-genomics",
    "lsmc",
    "rcrf",
    "budget_tags/",
)

VERSION_INFO_KEY = "s3_reference_data_version.info"
