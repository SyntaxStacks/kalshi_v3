## Operator UI Notes

### Information hierarchy
- The mobile-first order is now: top decision bar, lane triage, live exceptions and actions, current exposure, execution diagnostics, operator and exchange state, opportunities, then bankroll detail.
- This is intentionally risk-first. The operator should see safety, live state, and lane capitalworthiness before diagnostic detail or accounting detail.

### Mobile-first rationale
- The top decision bar is designed to answer the first-screen questions quickly: live on/off, exchange truth, execution truth, live capital at risk, and whether action is needed.
- Lane truth moved ahead of generic readiness so operators evaluate specific lanes before they evaluate broad system status.
- Emergency and risk controls are visually separated from maintenance actions so dangerous actions are harder to hit casually on a phone.

### Diagnostic humility
- Any weak live or replay sample is labeled plainly as `Insufficient Sample` or `Diagnostic Only`.
- Execution diagnostics use neutral treatment until live evidence is strong enough to act on.
- Lane triage cards avoid optimistic wording when a lane has not earned enough live evidence to justify capital.
