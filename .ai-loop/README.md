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
- `loop_state.json`
  - current loop state used by the Discord bot for proactive notifications
- `logs/`
  - stdout/stderr and prompt files for each iteration
- `idle_backlog.json`
  - deterministic backlog of profit-focused idle tranches
- `discord_operator.py`
  - optional host-side Discord bot for `kalshi-v3` runtime + loop control
- `requirements.txt`
  - Python dependency for the host-side Discord bot

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
$env:CODEX_COMMAND='type "{prompt_file}" | "C:\nvm4w\nodejs\codex.CMD" exec -C "{repo_path}" --dangerously-bypass-approvals-and-sandbox -o "{output_file}" -'
python .ai-loop\run_loop.py
```

This repo now includes `.ai-loop/config.json` with a working Windows-local Codex command for this machine. Override it only if your local Codex path or flags differ.

## Discord Operator

This repo can expose a small host-side Discord bot for the `kalshi-v3` loop.

Why host-side:
- the AI loop lives in `.ai-loop/` on the host
- Codex CLI is host-side, not in the Docker containers
- the bot should talk to both:
  - the live `kalshi-v3` API at `http://127.0.0.1:8080`
  - the local `.ai-loop` files

Setup:

```powershell
python -m pip install -r .ai-loop\requirements.txt
python .ai-loop\discord_operator.py
```

The bot reads these env vars from the repo `.env`:
- `DISCORD_BOT_TOKEN` or `DISCORD_BOT_TOKEN_FILE`
- `DISCORD_BOT_GUILD_ID`
- `DISCORD_BOT_ALLOWED_CHANNEL_IDS`
- `DISCORD_BOT_ALLOWED_ROLE_IDS`

Optional:
- `DISCORD_OPERATOR_API_BASE`
  - defaults to `http://127.0.0.1:8080`

Current commands:
- `/ai status`
  - reads `kalshi-v3` runtime from `/v1/runtime`
- `/ai loop_status`
  - reads the local `.ai-loop` artifacts
- `/ai approve_loop`
  - writes `.ai-loop/operator_input.json` with `approved=true`
- `/ai approve_loop <note>`
  - writes `.ai-loop/operator_input.json` with `approved=true` and operator notes
- `/ai run_loop`
  - launches `.ai-loop/run_loop.py` on the host via the local Python/Codex CLI environment
- `/ai run_loop <note>`
  - writes operator notes first, then launches the loop
- `/ai stop_loop`
  - writes `.ai-loop/operator_input.json` with `stop=true`

Proactive behavior:
- the bot watches `loop_state.json` and pushes loop updates into the allowed Discord channel
- when enabled, the bot also checks `idle_backlog.json` and can launch the next profit-focused tranche automatically when:
  - no loop is currently running
  - `NEEDS_INPUT.md` is absent
  - the repo worktree is clean

Idle improvement is intentionally bounded:
- it only launches specs from `idle_backlog.json`
- each entry has deterministic conditions, cooldown, and max-runs
- it does not invent new scope on its own

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
