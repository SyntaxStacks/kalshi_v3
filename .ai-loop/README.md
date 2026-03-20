# AI Loop

Repo-local review and implementation loop for `kalshi-v3`.

## Purpose

This loop is built for:
- bounded Codex implementation passes
- optional reviewer passes
- structured JSON artifacts
- Discord webhook notifications
- file-based human input

Discord is notification-only here. A plain webhook cannot receive replies back into control flow.

## Files

- `review_spec.md`
  - the active tranche/spec
- `operator_input.json`
  - optional human input file consumed on the next run
- `codex_result.json`
  - structured output from the Codex pass
- `reviewer_result.json`
  - structured output from the reviewer pass
- `metrics_snapshot.json`
  - optional runtime metrics scaffold
- `NEEDS_INPUT.md`
  - written when the loop pauses for human action
- `logs/`
  - stdout/stderr and prompt files for each iteration

## Setup

Use either environment variables or `.ai-loop/config.json`.

Supported config:
- `DISCORD_WEBHOOK_URL`
- `REPO_PATH`
- `MAX_ITERATIONS`
- `CODEX_COMMAND`
- `REVIEWER_COMMAND`
- `BRANCH_NAME`

Example:

```powershell
$env:DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
$env:REPO_PATH="C:\code\kalshi-v3"
$env:MAX_ITERATIONS="2"
$env:CODEX_COMMAND='codex exec --cwd {repo_path} --output-file {output_file} --input-file {prompt_file}'
python .ai-loop\run_loop.py
```

The default `CODEX_COMMAND` is only a placeholder. Adjust it to match the Codex CLI installed on your machine.

## Human Input

Create `.ai-loop/operator_input.json` from the example file and edit it before the next run.

Shape:

```json
{
  "approved": true,
  "notes": "",
  "scope_adjustments": [],
  "stop": false
}
```

Behavior:
- `stop=true` pauses the loop immediately
- `notes` and `scope_adjustments` are embedded into the next Codex prompt

## Discord Notifications

The loop posts concise webhook notifications for:
- loop started
- Codex pass completed
- reviewer pass completed
- human input needed
- loop finished

It does not ask Discord for instructions and it does not wait for Discord replies.

## Loop Lifecycle

1. Read `review_spec.md`
2. Read `operator_input.json` if present
3. Build a Codex prompt and run the configured Codex command
4. Validate `codex_result.json` against `codex_result.schema.json`
5. Optionally run the reviewer command
6. Validate `reviewer_result.json`
7. Post a concise Discord update
8. Pause on:
   - invalid JSON
   - failed command
   - `needs_input=true`
   - `operator_input.stop=true`
9. Stop on:
   - reviewer reports no critical blockers in scope
   - max iterations reached
   - Codex completed and no reviewer is configured

## Notes

- The loop is repo-local only.
- It does not auto-merge.
- It does not auto-push.
- `review_spec.md` is the only active tranche spec by convention.
- A practical follow-up convention is to promote `reviewer_result.json` into the next `review_spec.md`.
