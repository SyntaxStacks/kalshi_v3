# Idle Tranche: Blocked Opportunity Audit

Goal:
Increase profitable trade approvals by finding blocked opportunity reasons that are firing incorrectly or too conservatively.

Priority reasons to audit:
- `edge_below_threshold`
- `low_confidence`
- `execution_score_below_threshold`
- `too_close_to_expiry`
- `lane_untrained`
- `lane_not_paper_ready`
- `size_too_small_for_live_contract`

Scope:
- `crates/decision/src/main.rs`
- `crates/storage/src/pg.rs`

Focus:
- false negatives, not model expansion
- deterministic threshold or lookup fixes
- exact tests for every blocker removed

Do not:
- weaken explicit risk controls
- add new strategies or families
- refactor broadly

Deliver:
- JSON output matching `.ai-loop/codex_result.schema.json`
- files changed
- exact behavior changes
- tests added
- remaining risks
