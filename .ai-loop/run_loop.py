#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
LOGS_DIR = ROOT / "logs"
SPEC_FILE = ROOT / "review_spec.md"
OPERATOR_INPUT_FILE = ROOT / "operator_input.json"
CODEX_RESULT_FILE = ROOT / "codex_result.json"
REVIEWER_RESULT_FILE = ROOT / "reviewer_result.json"
METRICS_SNAPSHOT_FILE = ROOT / "metrics_snapshot.json"
NEEDS_INPUT_FILE = ROOT / "NEEDS_INPUT.md"
LOOP_STATE_FILE = ROOT / "loop_state.json"
CONFIG_FILE = ROOT / "config.json"
CONFIG_EXAMPLE_FILE = ROOT / "config.example.json"
CODEX_SCHEMA_FILE = ROOT / "codex_result.schema.json"
REVIEWER_SCHEMA_FILE = ROOT / "reviewer_result.schema.json"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def ensure_dirs() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def load_json_file(path: pathlib.Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json_file(path: pathlib.Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def write_text_file(path: pathlib.Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def clear_file(path: pathlib.Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def load_config() -> dict[str, Any]:
    file_config = load_json_file(CONFIG_FILE, default={}) or {}
    example_config = load_json_file(CONFIG_EXAMPLE_FILE, default={}) or {}
    config = {
        "discord_webhook_url": os.environ.get(
            "DISCORD_WEBHOOK_URL",
            file_config.get("discord_webhook_url", example_config.get("discord_webhook_url")),
        ),
        "repo_path": os.environ.get(
            "REPO_PATH",
            file_config.get("repo_path", str(REPO_ROOT)),
        ),
        "max_iterations": int(
            os.environ.get(
                "MAX_ITERATIONS",
                file_config.get("max_iterations", example_config.get("max_iterations", 1)),
            )
        ),
        "codex_command": os.environ.get(
            "CODEX_COMMAND",
            file_config.get(
                "codex_command",
                "codex exec --cwd {repo_path} --output-file {output_file} --input-file {prompt_file}",
            ),
        ),
        "reviewer_command": os.environ.get(
            "REVIEWER_COMMAND",
            file_config.get("reviewer_command", example_config.get("reviewer_command")),
        ),
        "branch_name": os.environ.get(
            "BRANCH_NAME",
            file_config.get("branch_name", example_config.get("branch_name", "")),
        ),
        "auto_push": bool(
            file_config.get("auto_push", False)
            or str(os.environ.get("AUTO_PUSH", "false")).lower() in {"1", "true", "yes"}
        ),
    }
    return config


def write_loop_state(
    status: str,
    *,
    iteration: int | None = None,
    details: str = "",
    needs_input: bool = False,
    codex_status: str = "",
    files_changed: int | None = None,
    verification: list[Any] | None = None,
) -> None:
    payload = {
        "updated_at": utc_now(),
        "status": status,
        "iteration": iteration,
        "details": details,
        "needs_input": needs_input,
        "codex_status": codex_status,
        "files_changed": files_changed,
        "verification": verification or [],
    }
    write_json_file(LOOP_STATE_FILE, payload)


def render_command(template: str, **values: str) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace("{" + key + "}", value)
    return rendered


def run_command(command: str, cwd: pathlib.Path, stdout_path: pathlib.Path, stderr_path: pathlib.Path) -> int:
    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_handle:
        process = subprocess.run(
            command,
            cwd=str(cwd),
            shell=True,
            text=True,
            stdout=stdout_handle,
            stderr=stderr_handle,
            check=False,
        )
    return process.returncode


def validate_type(value: Any, expected: str) -> bool:
    if expected == "string":
        return isinstance(value, str)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "array":
        return isinstance(value, list)
    if expected == "object":
        return isinstance(value, dict)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "null":
        return value is None
    return True


def validate_against_schema(payload: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    errors: list[str] = []
    schema_type = schema.get("type")
    if schema_type and not validate_type(payload, schema_type):
        return [f"{path}: expected {schema_type}, got {type(payload).__name__}"]

    if schema_type == "object":
        required = schema.get("required", [])
        properties = schema.get("properties", {})
        for key in required:
            if key not in payload:
                errors.append(f"{path}.{key}: missing required key")
        for key, value in payload.items():
            if key in properties:
                errors.extend(validate_against_schema(value, properties[key], f"{path}.{key}"))
    elif schema_type == "array":
        item_schema = schema.get("items", {})
        for index, item in enumerate(payload):
            errors.extend(validate_against_schema(item, item_schema, f"{path}[{index}]"))
    return errors


def post_discord(webhook_url: str | None, content: str) -> None:
    if not webhook_url:
        return
    payload = json.dumps({"content": content}).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10):
            return
    except urllib.error.URLError:
        return


def read_operator_input() -> dict[str, Any]:
    payload = load_json_file(
        OPERATOR_INPUT_FILE,
        default={"approved": True, "notes": "", "scope_adjustments": [], "stop": False},
    )
    if not isinstance(payload, dict):
        return {"approved": True, "notes": "", "scope_adjustments": [], "stop": False}
    return {
        "approved": bool(payload.get("approved", True)),
        "notes": str(payload.get("notes", "")),
        "scope_adjustments": list(payload.get("scope_adjustments", [])),
        "stop": bool(payload.get("stop", False)),
    }


def ensure_metrics_snapshot() -> None:
    if METRICS_SNAPSHOT_FILE.exists():
        return
    write_json_file(
        METRICS_SNAPSHOT_FILE,
        {
            "approved_decisions": None,
            "intents_inserted": None,
            "execution_score_rejections": None,
            "duplicate_suppression": None,
            "trades_opened": None,
            "updated_at": utc_now(),
            "notes": "Populate from runtime or SQL queries when available.",
        },
    )


def build_codex_prompt(spec_body: str, operator_input: dict[str, Any], schema_path: pathlib.Path) -> str:
    return (
        "You are running a bounded implementation pass for kalshi-v3.\n\n"
        "Rules:\n"
        "- Work only within the repo scope described below.\n"
        "- Return JSON only.\n"
        "- The JSON must validate against the schema file path below.\n"
        "- Do not include markdown fences.\n"
        "- Set needs_input=true only if human clarification is actually required.\n\n"
        f"Schema file: {schema_path}\n\n"
        "Operator input:\n"
        f"{json.dumps(operator_input, indent=2)}\n\n"
        "Current tranche spec:\n"
        f"{spec_body}\n"
    )


def build_reviewer_prompt(
    spec_body: str,
    codex_result: dict[str, Any],
    schema_path: pathlib.Path,
) -> str:
    return (
        "You are reviewing a bounded implementation pass for kalshi-v3.\n\n"
        "Rules:\n"
        "- Return JSON only.\n"
        "- The JSON must validate against the schema file path below.\n"
        "- Do not include markdown fences.\n"
        "- Focus on blockers in the current tranche scope.\n\n"
        f"Schema file: {schema_path}\n\n"
        "Current tranche spec:\n"
        f"{spec_body}\n\n"
        "Implementation result:\n"
        f"{json.dumps(codex_result, indent=2)}\n"
    )


def write_needs_input(reason: str, details: str) -> None:
    body = (
        "# Needs Input\n\n"
        f"- Time: `{utc_now()}`\n"
        f"- Reason: `{reason}`\n\n"
        "## Details\n\n"
        f"{details}\n"
    )
    write_text_file(NEEDS_INPUT_FILE, body)
    write_loop_state(
        "needs_input",
        details=reason,
        needs_input=True,
    )


def summarize_test_verification(verification: list[Any]) -> str:
    joined = " | ".join(str(item) for item in verification[:4]) if verification else "none"
    return joined[:300]


def should_stop_from_reviewer(reviewer_result: dict[str, Any] | None) -> bool:
    if not reviewer_result:
        return False
    blockers = reviewer_result.get("critical_trade_blockers", [])
    return isinstance(blockers, list) and len(blockers) == 0


def loop() -> int:
    ensure_dirs()
    ensure_metrics_snapshot()
    config = load_config()
    repo_path = pathlib.Path(config["repo_path"]).resolve()
    operator_input = read_operator_input()
    clear_file(NEEDS_INPUT_FILE)
    write_loop_state("starting", details="Preparing loop run.")

    if operator_input["stop"]:
        write_needs_input("operator_stop", "Operator requested stop via `.ai-loop/operator_input.json`.")
        post_discord(config["discord_webhook_url"], "AI loop paused: operator requested stop.")
        return 0

    if not SPEC_FILE.exists():
        write_needs_input("missing_spec", "Create `.ai-loop/review_spec.md` before running the loop.")
        post_discord(config["discord_webhook_url"], "AI loop paused: missing `.ai-loop/review_spec.md`.")
        return 1

    spec_body = SPEC_FILE.read_text(encoding="utf-8")
    branch_note = f" on `{config['branch_name']}`" if config.get("branch_name") else ""
    post_discord(
        config["discord_webhook_url"],
        f"AI loop started{branch_note}. max_iterations={config['max_iterations']}",
    )
    write_loop_state("running", details="Loop started.", iteration=0)

    for iteration in range(1, int(config["max_iterations"]) + 1):
        prompt_file = LOGS_DIR / f"iteration-{iteration:02d}-codex-prompt.txt"
        codex_stdout = LOGS_DIR / f"iteration-{iteration:02d}-codex.stdout.log"
        codex_stderr = LOGS_DIR / f"iteration-{iteration:02d}-codex.stderr.log"
        reviewer_prompt_file = LOGS_DIR / f"iteration-{iteration:02d}-reviewer-prompt.txt"
        reviewer_stdout = LOGS_DIR / f"iteration-{iteration:02d}-reviewer.stdout.log"
        reviewer_stderr = LOGS_DIR / f"iteration-{iteration:02d}-reviewer.stderr.log"

        write_text_file(
            prompt_file,
            build_codex_prompt(spec_body, operator_input, CODEX_SCHEMA_FILE),
        )
        write_loop_state(
            "running",
            iteration=iteration,
            details="Running Codex pass.",
        )
        codex_command = render_command(
            config["codex_command"],
            repo_path=str(repo_path),
            prompt_file=str(prompt_file),
            output_file=str(CODEX_RESULT_FILE),
            iteration=str(iteration),
            spec_file=str(SPEC_FILE),
            operator_input_file=str(OPERATOR_INPUT_FILE),
        )
        codex_returncode = run_command(codex_command, repo_path, codex_stdout, codex_stderr)
        if codex_returncode != 0:
            write_needs_input(
                "codex_failed",
                f"Codex command failed with exit code {codex_returncode}. See `{codex_stderr}`.",
            )
            post_discord(
                config["discord_webhook_url"],
                f"AI loop iteration {iteration} failed during Codex run. human input needed.",
            )
            return 1

        try:
            codex_result = load_json_file(CODEX_RESULT_FILE, default=None)
        except json.JSONDecodeError as error:
            write_needs_input("invalid_codex_json", f"Invalid `codex_result.json`: {error}")
            post_discord(
                config["discord_webhook_url"],
                f"AI loop iteration {iteration}: invalid Codex JSON output.",
            )
            return 1

        schema = load_json_file(CODEX_SCHEMA_FILE, default={})
        errors = validate_against_schema(codex_result, schema)
        if errors:
            write_needs_input("codex_schema_failure", "\n".join(errors))
            post_discord(
                config["discord_webhook_url"],
                f"AI loop iteration {iteration}: Codex output failed schema validation.",
            )
            return 1

        post_discord(
            config["discord_webhook_url"],
            (
                f"Codex pass completed iter={iteration} "
                f"status={codex_result.get('status')} "
                f"files={len(codex_result.get('files_changed', []))} "
                f"verification={summarize_test_verification(codex_result.get('verification', []))}"
            ),
        )
        write_loop_state(
            "running",
            iteration=iteration,
            details="Codex pass completed.",
            codex_status=str(codex_result.get("status") or ""),
            files_changed=len(codex_result.get("files_changed", [])),
            verification=list(codex_result.get("verification") or []),
        )

        reviewer_result: dict[str, Any] | None = None
        reviewer_command = config.get("reviewer_command")
        if reviewer_command:
            write_text_file(
                reviewer_prompt_file,
                build_reviewer_prompt(spec_body, codex_result, REVIEWER_SCHEMA_FILE),
            )
            rendered_reviewer_command = render_command(
                reviewer_command,
                repo_path=str(repo_path),
                prompt_file=str(reviewer_prompt_file),
                output_file=str(REVIEWER_RESULT_FILE),
                iteration=str(iteration),
                spec_file=str(SPEC_FILE),
                codex_result_file=str(CODEX_RESULT_FILE),
            )
            reviewer_returncode = run_command(
                rendered_reviewer_command,
                repo_path,
                reviewer_stdout,
                reviewer_stderr,
            )
            if reviewer_returncode != 0:
                write_needs_input(
                    "reviewer_failed",
                    f"Reviewer command failed with exit code {reviewer_returncode}. See `{reviewer_stderr}`.",
                )
                post_discord(
                    config["discord_webhook_url"],
                    f"AI loop iteration {iteration} failed during reviewer run.",
                )
                return 1

            try:
                reviewer_result = load_json_file(REVIEWER_RESULT_FILE, default=None)
            except json.JSONDecodeError as error:
                write_needs_input("invalid_reviewer_json", f"Invalid `reviewer_result.json`: {error}")
                post_discord(
                    config["discord_webhook_url"],
                    f"AI loop iteration {iteration}: invalid reviewer JSON output.",
                )
                return 1

            reviewer_schema = load_json_file(REVIEWER_SCHEMA_FILE, default={})
            reviewer_errors = validate_against_schema(reviewer_result, reviewer_schema)
            if reviewer_errors:
                write_needs_input("reviewer_schema_failure", "\n".join(reviewer_errors))
                post_discord(
                    config["discord_webhook_url"],
                    f"AI loop iteration {iteration}: reviewer output failed schema validation.",
                )
                return 1

            post_discord(
                config["discord_webhook_url"],
                (
                    f"Reviewer pass completed iter={iteration} "
                    f"approved={reviewer_result.get('approved')} "
                    f"critical={len(reviewer_result.get('critical_trade_blockers', []))}"
                ),
            )

        if codex_result.get("needs_input"):
            write_needs_input(
                "codex_requested_input",
                str(codex_result.get("input_request", "Codex requested human input.")),
            )
            post_discord(
                config["discord_webhook_url"],
                f"AI loop iteration {iteration} paused: human input needed.",
            )
            return 0

        if should_stop_from_reviewer(reviewer_result):
            clear_file(NEEDS_INPUT_FILE)
            write_loop_state(
                "completed",
                iteration=iteration,
                details="Reviewer reports no critical blockers in scope.",
                codex_status=str(codex_result.get("status") or ""),
                files_changed=len(codex_result.get("files_changed", [])),
                verification=list(codex_result.get("verification") or []),
            )
            post_discord(
                config["discord_webhook_url"],
                f"AI loop finished at iteration {iteration}: reviewer reports no critical blockers in scope.",
            )
            return 0

        if not reviewer_command and codex_result.get("status") == "completed":
            clear_file(NEEDS_INPUT_FILE)
            write_loop_state(
                "completed",
                iteration=iteration,
                details="Codex completed and no reviewer is configured.",
                codex_status=str(codex_result.get("status") or ""),
                files_changed=len(codex_result.get("files_changed", [])),
                verification=list(codex_result.get("verification") or []),
            )
            post_discord(
                config["discord_webhook_url"],
                f"AI loop finished at iteration {iteration}: Codex completed and no reviewer is configured.",
            )
            return 0

    write_needs_input(
        "max_iterations_reached",
        f"Loop stopped after {config['max_iterations']} iterations.",
    )
    post_discord(
        config["discord_webhook_url"],
        f"AI loop stopped: max_iterations={config['max_iterations']} reached.",
    )
    return 0


if __name__ == "__main__":
    sys.exit(loop())
