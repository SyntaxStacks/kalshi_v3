# Idle Tranche: Profit Funnel Audit

Goal:
Find false-negative blockers in the path:

`approved decisions -> intents inserted -> execution-score rejections -> duplicate suppression -> trades opened`

Focus:
- decision/storage/execution code paths that suppress valid approved trades
- counters and queries that can prove where approved setups disappear
- changes that improve realized trading frequency without weakening explicit risk caps

Scope:
- `crates/decision/src/main.rs`
- `crates/storage/src/pg.rs`
- `crates/execution/src/main.rs`

Do:
- inspect funnel drop-off points
- prefer fixes that increase valid trade conversion
- add exact acceptance tests for any fix

Do not:
- add new strategies
- add cosmetic UI work
- weaken live risk controls
- broaden to unrelated cleanup

Deliver:
- JSON output matching `.ai-loop/codex_result.schema.json`
- files changed
- exact behavior changes
- tests added
- remaining risks
