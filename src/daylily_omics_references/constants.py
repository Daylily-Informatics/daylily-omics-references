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
    "cluster_boot_config/",
    "data/cached_envs/",
    "data/libs/",
    "data/tool_specific_resources/",
    "data/budget_tags/",
)

# Optional prefixes that may be toggled via CLI flags.
HG38_PREFIXES = (
    "data/genomic_data/organism_references/H_sapiens/hg38/",
    "data/genomic_data/organism_annotations/H_sapiens/hg38/",
)

B37_PREFIXES = (
    "data/genomic_data/organism_references/H_sapiens/b37/",
    "data/genomic_data/organism_annotations/H_sapiens/b37/",
)

GIAB_PREFIXES = (
    "data/genomic_data/organism_reads/",
)

VERSION_INFO_KEY = "s3_reference_data_version.info"
