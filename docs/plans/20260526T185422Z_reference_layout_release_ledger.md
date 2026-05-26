# Daylily Omics References Layout Release Ledger

Controlling plan: DayRef side of the 15-agent bucket contract release and `bucketsamok` validation.
Ledger path: `docs/plans/20260526T185422Z_reference_layout_release_ledger.md`
Created: 2026-05-26T18:54:22Z

## Gate 0 Baseline

- Repo: `/Users/jmajor/projects/daylily/daylily-omics-references`
- Branch/head: `codex/headnode-readiness-reference-verifier`, `eca57742496496f545a1841e9d287f4a4ca4b851`
- Dirty state at Gate 0: clean.
- Latest local tag: `0.3.5`.
- Sweep command: `rg -n "reference_bucket|control_data_bucket|stage_bucket|--reference-bucket|--control-data-bucket|--stage-bucket|bucket-or-prefix|/fsx/runtime_assets|/fsx/data|/data/staged_sample_data|lsmc-dayoa-omics-analysis-us-west-2" . -S -g '!**/.git/**' -g '!docs/plans/**'`
- Gate 0 hits: README references `/fsx/data`; source/tests contain internal `clone_reference_bucket` API naming that must be reviewed before renaming because it may mean a real bucket object, not a public `s3://...` field.

## Control Ledger

| ID | Agent | Area | Requirement | Status | Category | Approval Gate | Evidence | Root Cause | Terminal Note |
|---|---|---|---|---|---|---|---|---|---|
| DAYREF-BKT-001 | Agent 11 | Layout docs | Replace old `/fsx/data` readiness/layout wording with `/fsx/references` contract. | SUCCESS | feature_implementation | Gate 1 | README readiness wording now says `/fsx/references`. |  | Stale README path removed. |
| DAYREF-BKT-002 | Agent 11 | Public scrub | Add or update validation that rejects LSMC/RCRF/license/private material in public-safe mirrors. | SUCCESS | contract_test | Gate 1 | Existing `verify --public-safe` and tests reject `.lic`, `license`, `sentieon-genomics`, `lsmc`, `rcrf`, `budget_tags/`, private, and commercial keys; `python -m pytest -q tests/test_manager.py -> 21 passed`. |  | Public-scrub validation already implemented by prior layout ledger and retained. |
| DAYREF-BKT-003 | Agent 11 | Runtime assets | Represent public-safe runtime helpers under `runtime_assets/...` and references under top-level `genomic_data/...`. | SUCCESS | feature_implementation | Gate 1 | `src/daylily_omics_references/constants.py` expects `runtime_assets/...`, top-level `genomic_data/...`, and `genomic_data/organism_reads_slim/`; shell helper updated to the same layout; tests assert old `data/genomic_data/` is absent; `python -m pytest -q tests/test_manager.py -> 21 passed`. |  | Layout matches the one-DRA references contract and slim-read reference subtree. |
| DAYREF-BKT-004 | Agent 11 | API naming | Inspect `clone_reference_bucket` naming and only rename if it is a public S3 URI contract conflict. | NO_LONGER_NEEDED | active_product_contract | Gate 1 | `clone_reference_bucket` creates/clones a real S3 bucket and returns a bucket name; it does not accept the removed public `*_bucket=s3://...` config pattern. |  | API name remains because it accurately describes bucket management. |
| DAYREF-BKT-005 | Agent 13 | Release | PR-merge/tag DayRef before downstream pins when layout tests pass. | BLOCKED | config_or_startup_contract | Release gate | Local latest tag `0.3.5`. | Requires implementation and tests. |  |

## Acceptance Checks

- DayRef active docs/tests reflect `genomic_data/...`, `genomic_data/organism_reads_slim/...`, and `runtime_assets/...`.
- Public scrub tests fail on LSMC, RCRF, license, Sentieon install tree, and internal budget tag material.
- DayRef release tag is cut before DayEC/Ursa pins consume it.
