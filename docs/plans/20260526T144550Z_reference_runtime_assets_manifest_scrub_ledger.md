# References Repo Runtime Assets Manifest And Scrub Ledger

Created: 2026-05-26T14:45:50Z

## Objective

Update `daylily-omics-references` for the new top-level reference layout and
runtime-assets prefix. The verifier must support private LSMC-style references
and public Daylily-safe references, and public verification must reject licenses,
private/commercial assets, and LSMC/RCRF identifiers.

## Gate 0 Inventory

- Repo: `/Users/jmajor/projects/daylily/daylily-omics-references`.
- Git state: `## codex/headnode-readiness-reference-verifier...origin/codex/headnode-readiness-reference-verifier`.
- Initial sweep: `rg -n "data/cached_envs|data/tool_specific_resources|data/budget_tags|data/genomic_data|runtime_assets|lsmc|rcrf|lic" src tests` found 33 hits.
- Current constants still expect `data/cached_envs/`, `data/tool_specific_resources/`, `data/budget_tags/`, and `data/genomic_data/...`.
- Target layout: `runtime_assets/...` and top-level `genomic_data/...`.
- Public mirror rule: strip `*.lic`, `*license*`, Sentieon install trees, LSMC/RCRF-named assets, and internal budget tags.

## Ledger

| ID | Area | Requirement | Status | Category | Gate | Owner | Evidence | Root Cause | Terminal Note |
|---|---|---|---|---|---|---|---|---|---|
| DOR-RRA-001 | Inventory | Record reference-verifier baseline. | SUCCESS | contract_test | Gate 0 | orchestrator | Gate 0 section. |  | Baseline recorded. |
| DOR-RRA-002 | Layout constants | Replace old `data/...` runtime/reference expectations with `runtime_assets/...` and top-level `genomic_data/...`. | SUCCESS | feature_implementation | Gate 1 | Agent E | `src/daylily_omics_references/constants.py` now requires `runtime_assets/{cluster_boot_config,cached_envs,tool_specific_resources,budget_tags}/`, DAY-EC object keys under `runtime_assets/...`, and top-level `genomic_data/...` prefixes. |  | Verifier contract matches one-DRA references layout. |
| DOR-RRA-003 | Public scrub | Add public-safe validation rejecting LSMC/RCRF/license/private/commercial runtime assets. | SUCCESS | feature_implementation | Gate 1 | Agent E | `verify --public-safe` calls `verify_bucket(..., public_safe=True)`; manager scans `runtime_assets/` keys and rejects `.lic`, `license`, `sentieon-genomics`, `lsmc`, `rcrf`, `budget_tags/`, private, and commercial key names. |  | Public Daylily-safe references can be validated separately from private LSMC references. |
| DOR-RRA-004 | Tests | Add tests for private LSMC-style references and public Daylily-safe references. | SUCCESS | contract_test | Gate 2 | Agent E | `python -m pytest -q` returned `21 passed`; tests cover private runtime assets allowed without `public_safe`, public-safe rejection, and public helper acceptance. |  | References repo local validation is green. |
| DOR-RRA-005 | Daylily mirror validation | After LSMC validation passes, verify `daylily-dayoa-references-usw2/runtime_assets/` contains only public-safe assets. | OPEN | contract_test | Gate 7 | Agent H |  |  |  |

## Acceptance Notes

- Do not publish license files, Sentieon install trees, or LSMC/RCRF/internal budget material into public Daylily references.
- Do not delete standalone Daylily buckets without later exact destructive approval.
