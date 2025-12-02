#!/usr/bin/env bash
set -euo pipefail

# Shell-based replacement for the daylily-omics-references CLI.
# Relies solely on the AWS CLI and standard Unix utilities.

DEFAULT_REFERENCE_VERSION="0.7.131c"
VERSION_INFO_KEY="s3_reference_data_version.info"

# Map reference versions to source buckets (kept compatible with older Bash versions).
get_source_bucket_for_version() {
  local version="$1"
  case "$version" in
    "$DEFAULT_REFERENCE_VERSION")
      printf '%s\n' "daylily-omics-analysis-references-public"
      ;;
    *)
      return 1
      ;;
  esac
}

CORE_PREFIXES=(
  "cluster_boot_config/"
  "data/cached_envs/"
  "data/tool_specific_resources/"
  "data/budget_tags/"
)
HG38_PREFIXES=(
  "data/genomic_data/organism_references/H_sapiens/hg38/"
  "data/genomic_data/organism_annotations/H_sapiens/hg38/"
)
B37_PREFIXES=(
  "data/genomic_data/organism_references/H_sapiens/b37/"
  "data/genomic_data/organism_annotations/H_sapiens/b37/"
)
GIAB_PREFIXES=(
  "data/genomic_data/organism_reads/"
)

log() {
  local level="$1"; shift
  printf '[%s] %s\n' "$level" "$*" >&2
}

require_binary() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log ERROR "Missing required command: $1"
    exit 1
  fi
}

set_aws_env() {
  local profile="$1"
  local region="$2"
  if [[ -n "$profile" ]]; then
    export AWS_PROFILE="$profile"
  fi
  if [[ -n "$region" ]]; then
    export AWS_REGION="$region"
    export AWS_DEFAULT_REGION="$region"
  fi
}

bucket_exists() {
  local bucket="$1"
  if aws s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

create_bucket() {
  local bucket="$1" region="$2" dry_run="$3"

  if [[ "$dry_run" == true ]]; then
    log INFO "[dry-run] Would create bucket $bucket in $region"
    return
  fi

  local args=("--bucket" "$bucket")
  if [[ "$region" != "us-east-1" ]]; then
    args+=("--create-bucket-configuration" "LocationConstraint=$region")
  fi

  log INFO "Creating bucket $bucket in $region"
  aws s3api create-bucket "${args[@]}"
  log INFO "Waiting for bucket $bucket to become listable"
  aws s3api wait bucket-exists --bucket "$bucket"
  aws s3api put-bucket-accelerate-configuration --bucket "$bucket" --accelerate-configuration Status=Enabled

  local account_id
  account_id=$(aws sts get-caller-identity --query Account --output text)
  local policy
  policy=$(cat <<POLICY
{"Version":"2012-10-17","Statement":[{"Sid":"EnforceTLS","Effect":"Deny","Principal":"*","Action":"s3:*","Resource":["arn:aws:s3:::$bucket","arn:aws:s3:::$bucket/*"],"Condition":{"Bool":{"aws:SecureTransport":"false"}}},{"Sid":"AllowAccountFullAccess","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::$account_id:root"},"Action":"s3:*","Resource":["arn:aws:s3:::$bucket","arn:aws:s3:::$bucket/*"]}]}
POLICY
)
  aws s3api put-bucket-policy --bucket "$bucket" --policy "$policy"
}

wait_for_bucket_listable() {
  local bucket="$1" attempts="$2" delay="$3"
  local i
  for ((i=1; i<=attempts; i++)); do
    if aws s3api list-objects-v2 --bucket "$bucket" --max-keys 1 >/dev/null 2>&1; then
      return 0
    fi
    if [[ "$i" -eq "$attempts" ]]; then
      log ERROR "Bucket $bucket is not listable after creation"
      exit 1
    fi
    sleep "$delay"
  done
}

write_version_file() {
  local bucket="$1" version="$2" dry_run="$3"
  if [[ "$dry_run" == true ]]; then
    log INFO "[dry-run] Would upload $VERSION_INFO_KEY with version $version"
    return
  fi
  printf '%s' "$version" | aws s3 cp - "s3://$bucket/$VERSION_INFO_KEY"
}

copy_prefix() {
  local source_bucket="$1" dest_bucket="$2" prefix="$3" dry_run="$4" use_accel="$5" log_file="$6"
  local endpoint_args=()
  if [[ "$use_accel" == true ]]; then
    endpoint_args=("--endpoint-url" "https://s3-accelerate.amazonaws.com")
  fi
  local cmd=(aws s3 cp "s3://$source_bucket/$prefix" "s3://$dest_bucket/$prefix" --recursive --request-payer requester --metadata-directive REPLACE "${endpoint_args[@]}")

  if [[ "$dry_run" == true ]]; then
    log INFO "[dry-run] ${cmd[*]}"
    return
  fi

  if [[ -n "$log_file" ]]; then
    mkdir -p "$(dirname "$log_file")"
    {
      printf '$ %s\n' "${cmd[*]}"
      "${cmd[@]}"
    } >> "$log_file" 2>&1
  else
    "${cmd[@]}"
  fi
}

prefix_exists() {
  local bucket="$1" prefix="$2"
  local output
  if ! output=$(aws s3api list-objects-v2 --bucket "$bucket" --prefix "$prefix" --max-keys 1); then
    return 1
  fi
  [[ $(echo "$output" | jq '.Contents | length') -gt 0 ]]
}

clone_bucket() {
  local bucket_prefix="$1" region="$2" version="$3" dry_run="$4" include_hg38="$5" include_b37="$6" include_giab="$7" use_accel="$8" log_file="$9"

  local source_bucket
  if ! source_bucket=$(get_source_bucket_for_version "$version"); then
    log ERROR "Unsupported reference version: $version"
    exit 1
  fi

  local bucket_name="${bucket_prefix}-omics-analysis-${region}"
  if bucket_exists "$bucket_name"; then
    log ERROR "Bucket '$bucket_name' already exists"
    exit 1
  fi

  create_bucket "$bucket_name" "$region" "$dry_run"
  if [[ "$dry_run" == false ]]; then
    wait_for_bucket_listable "$bucket_name" 5 1
  fi

  write_version_file "$bucket_name" "$version" "$dry_run"

  local plan=("${CORE_PREFIXES[@]}")
  [[ "$include_hg38" == true ]] && plan+=("${HG38_PREFIXES[@]}")
  [[ "$include_b37" == true ]] && plan+=("${B37_PREFIXES[@]}")
  [[ "$include_giab" == true ]] && plan+=("${GIAB_PREFIXES[@]}")

  local total=${#plan[@]} idx=1 prefix
  for prefix in "${plan[@]}"; do
    log INFO "Copying $prefix ($idx/$total)"
    copy_prefix "$source_bucket" "$bucket_name" "$prefix" "$dry_run" "$use_accel" "$log_file"
    ((idx++))
  done

  log INFO "Bucket $bucket_name is ready"
  printf '%s\n' "$bucket_name"
}

verify_bucket() {
  local bucket="$1" version="$2" include_hg38="$3" include_b37="$4" include_giab="$5"

  if ! bucket_exists "$bucket"; then
    log ERROR "Bucket '$bucket' does not exist"
    return 1
  fi

  local bucket_version
  if bucket_version=$(aws s3 cp "s3://$bucket/$VERSION_INFO_KEY" - 2>/dev/null); then
    bucket_version=$(echo "$bucket_version" | tr -d '\n' | tr -d '\r')
  else
    log ERROR "Missing version marker in $bucket"
    return 1
  fi

  if [[ "$bucket_version" != "$version" ]]; then
    log ERROR "Version mismatch: expected $version, found $bucket_version"
    return 1
  fi

  local prefixes=("${CORE_PREFIXES[@]}")
  [[ "$include_hg38" == true ]] && prefixes+=("${HG38_PREFIXES[@]}")
  [[ "$include_b37" == true ]] && prefixes+=("${B37_PREFIXES[@]}")
  [[ "$include_giab" == true ]] && prefixes+=("${GIAB_PREFIXES[@]}")

  local missing=()
  local p
  for p in "${prefixes[@]}"; do
    if ! prefix_exists "$bucket" "$p"; then
      missing+=("$p")
    fi
  done

  if [[ ${#missing[@]} -gt 0 ]]; then
    log ERROR "Bucket '$bucket' is missing expected prefixes: ${missing[*]}"
    return 1
  fi

  log INFO "Bucket $bucket passed verification for version $version"
}

ensure_bucket() {
  local bucket_prefix="$1" region="$2" version="$3" include_hg38="$4" include_b37="$5" include_giab="$6" use_accel="$7" log_file="$8" dry_run="$9" create_missing="${10}"
  local bucket_name="${bucket_prefix}-omics-analysis-${region}"
  if bucket_exists "$bucket_name"; then
    verify_bucket "$bucket_name" "$version" "$include_hg38" "$include_b37" "$include_giab"
    printf '%s\n' "$bucket_name"
    return
  fi

  if [[ "$create_missing" != true ]]; then
    log ERROR "Bucket '$bucket_name' is missing"
    exit 1
  fi

  log INFO "Bucket $bucket_name is missing; cloning reference data (dry_run=$dry_run)"
  clone_bucket "$bucket_prefix" "$region" "$version" "$dry_run" "$include_hg38" "$include_b37" "$include_giab" "$use_accel" "$log_file"
}

usage() {
  cat <<'USAGE'
Usage: daylily-omics-references.sh [GLOBAL OPTIONS] <command> [ARGS]

Global options:
  --profile PROFILE   AWS profile to use
  --region REGION     AWS region to target (required for clone/ensure)

Commands:
  clone   Create a new reference bucket from the public source
  verify  Validate that a bucket matches the expected structure
  ensure  Verify a bucket or create it if missing

Run "daylily-omics-references.sh <command> --help" for command-specific options.
USAGE
}

clone_help() {
  cat <<'USAGE'
Usage: clone --bucket-prefix PREFIX --region REGION [options]

Options:
  --bucket-prefix PREFIX   Prefix for the destination bucket name (required)
  --region REGION          AWS region for the destination bucket (required)
  --version VERSION        Reference version to copy (default: 0.7.131c)
  --include-hg38           Include hg38 data (default: enabled)
  --exclude-hg38           Skip hg38 data
  --include-b37            Include b37 data (default: enabled)
  --exclude-b37            Skip b37 data
  --include-giab           Include GIAB data (default: enabled)
  --exclude-giab           Skip GIAB data
  --execute                Perform actions (default is dry-run)
  --use-acceleration       Copy using the S3 accelerate endpoint
  --log-file PATH          Append AWS CLI output to PATH
USAGE
}

verify_help() {
  cat <<'USAGE'
Usage: verify --bucket NAME [options]

Options:
  --bucket NAME            Bucket to check (required)
  --version VERSION        Expected reference version (default: 0.7.131c)
  --include-hg38           Expect hg38 data (default: enabled)
  --exclude-hg38           Skip hg38 checks
  --include-b37            Expect b37 data (default: enabled)
  --exclude-b37            Skip b37 checks
  --include-giab           Expect GIAB data (default: enabled)
  --exclude-giab           Skip GIAB checks
USAGE
}

ensure_help() {
  cat <<'USAGE'
Usage: ensure --bucket-prefix PREFIX --region REGION [options]

Options:
  --bucket-prefix PREFIX   Prefix for the bucket name (required)
  --region REGION          AWS region for the bucket (required)
  --version VERSION        Reference version (default: 0.7.131c)
  --include-hg38           Include hg38 data (default: enabled)
  --exclude-hg38           Skip hg38 data
  --include-b37            Include b37 data (default: enabled)
  --exclude-b37            Skip b37 data
  --include-giab           Include GIAB data (default: enabled)
  --exclude-giab           Skip GIAB data
  --execute                Perform actions when creating buckets (default is dry-run)
  --use-acceleration       Copy using the S3 accelerate endpoint
  --log-file PATH          Append AWS CLI output to PATH
  --no-create              Fail if the bucket is missing instead of creating
USAGE
}

main() {
  require_binary aws
  local profile="" region=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --profile) profile="$2"; shift 2 ;;
      --region) region="$2"; shift 2 ;;
      clone|verify|ensure) cmd="$1"; shift; break ;;
      -h|--help) usage; exit 0 ;;
      *) log ERROR "Unknown option: $1"; usage; exit 1 ;;
    esac
  done

  if [[ -z "${cmd:-}" ]]; then
    usage; exit 1
  fi

  set_aws_env "$profile" "$region"

  case "$cmd" in
    clone)
      local bucket_prefix="" version="$DEFAULT_REFERENCE_VERSION" include_hg38=true include_b37=true include_giab=true dry_run=true use_accel=false log_file=""
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --bucket-prefix) bucket_prefix="$2"; shift 2 ;;
          --region) region="$2"; shift 2 ;;
          --version) version="$2"; shift 2 ;;
          --include-hg38) include_hg38=true; shift ;;
          --exclude-hg38) include_hg38=false; shift ;;
          --include-b37) include_b37=true; shift ;;
          --exclude-b37) include_b37=false; shift ;;
          --include-giab) include_giab=true; shift ;;
          --exclude-giab) include_giab=false; shift ;;
          --execute) dry_run=false; shift ;;
          --use-acceleration) use_accel=true; shift ;;
          --log-file) log_file="$2"; shift 2 ;;
          -h|--help) clone_help; exit 0 ;;
          *) log ERROR "Unknown option for clone: $1"; clone_help; exit 1 ;;
        esac
      done
      [[ -z "$bucket_prefix" || -z "$region" ]] && { log ERROR "--bucket-prefix and --region are required"; exit 1; }
      clone_bucket "$bucket_prefix" "$region" "$version" "$dry_run" "$include_hg38" "$include_b37" "$include_giab" "$use_accel" "$log_file"
      ;;
    verify)
      local bucket="" version="$DEFAULT_REFERENCE_VERSION" include_hg38=true include_b37=true include_giab=true
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --bucket) bucket="$2"; shift 2 ;;
          --version) version="$2"; shift 2 ;;
          --include-hg38) include_hg38=true; shift ;;
          --exclude-hg38) include_hg38=false; shift ;;
          --include-b37) include_b37=true; shift ;;
          --exclude-b37) include_b37=false; shift ;;
          --include-giab) include_giab=true; shift ;;
          --exclude-giab) include_giab=false; shift ;;
          -h|--help) verify_help; exit 0 ;;
          *) log ERROR "Unknown option for verify: $1"; verify_help; exit 1 ;;
        esac
      done
      [[ -z "$bucket" ]] && { log ERROR "--bucket is required"; exit 1; }
      verify_bucket "$bucket" "$version" "$include_hg38" "$include_b37" "$include_giab"
      ;;
    ensure)
      local bucket_prefix="" version="$DEFAULT_REFERENCE_VERSION" include_hg38=true include_b37=true include_giab=true use_accel=false log_file="" dry_run=true create_missing=true
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --bucket-prefix) bucket_prefix="$2"; shift 2 ;;
          --region) region="$2"; shift 2 ;;
          --version) version="$2"; shift 2 ;;
          --include-hg38) include_hg38=true; shift ;;
          --exclude-hg38) include_hg38=false; shift ;;
          --include-b37) include_b37=true; shift ;;
          --exclude-b37) include_b37=false; shift ;;
          --include-giab) include_giab=true; shift ;;
          --exclude-giab) include_giab=false; shift ;;
          --execute) dry_run=false; shift ;;
          --use-acceleration) use_accel=true; shift ;;
          --log-file) log_file="$2"; shift 2 ;;
          --no-create) create_missing=false; shift ;;
          -h|--help) ensure_help; exit 0 ;;
          *) log ERROR "Unknown option for ensure: $1"; ensure_help; exit 1 ;;
        esac
      done
      [[ -z "$bucket_prefix" || -z "$region" ]] && { log ERROR "--bucket-prefix and --region are required"; exit 1; }
      ensure_bucket "$bucket_prefix" "$region" "$version" "$include_hg38" "$include_b37" "$include_giab" "$use_accel" "$log_file" "$dry_run" "$create_missing"
      ;;
  esac
}

main "$@"
 
