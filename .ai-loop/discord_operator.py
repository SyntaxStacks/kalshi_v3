#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

import discord
from discord import app_commands


ROOT = pathlib.Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
ENV_FILE = REPO_ROOT / ".env"
CODEX_RESULT_FILE = ROOT / "codex_result.json"
REVIEWER_RESULT_FILE = ROOT / "reviewer_result.json"
OPERATOR_INPUT_FILE = ROOT / "operator_input.json"
NEEDS_INPUT_FILE = ROOT / "NEEDS_INPUT.md"
RUN_LOOP_PID_FILE = ROOT / "run_loop.pid"
RUN_LOOP_STDOUT_FILE = ROOT / "logs" / "run_loop.trigger.stdout.log"
RUN_LOOP_STDERR_FILE = ROOT / "logs" / "run_loop.trigger.stderr.log"


def load_dotenv(path: pathlib.Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_dotenv(ENV_FILE)


def parse_csv_ints(name: str) -> list[int]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return []
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


@dataclass(slots=True)
class Settings:
    token: str
    guild_id: int
    allowed_channels: list[int]
    allowed_roles: list[int]
    api_base: str


def load_settings() -> Settings:
    token = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
    token_file = os.environ.get("DISCORD_BOT_TOKEN_FILE", "").strip()
    if not token and token_file:
        token = pathlib.Path(token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN or DISCORD_BOT_TOKEN_FILE is required")
    guild_raw = os.environ.get("DISCORD_BOT_GUILD_ID", "").strip()
    if not guild_raw:
        raise RuntimeError("DISCORD_BOT_GUILD_ID is required")
    return Settings(
        token=token,
        guild_id=int(guild_raw),
        allowed_channels=parse_csv_ints("DISCORD_BOT_ALLOWED_CHANNEL_IDS"),
        allowed_roles=parse_csv_ints("DISCORD_BOT_ALLOWED_ROLE_IDS"),
        api_base=os.environ.get("DISCORD_OPERATOR_API_BASE", "http://127.0.0.1:8080").rstrip("/"),
    )


def load_json(path: pathlib.Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: pathlib.Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def fetch_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def authorization_error(settings: Settings, interaction: discord.Interaction) -> str | None:
    if interaction.guild_id != settings.guild_id:
        return "This bot only accepts commands in the configured server."
    if settings.allowed_channels and interaction.channel_id not in settings.allowed_channels:
        return "This command is not enabled in this channel."
    if settings.allowed_roles:
        roles = [role.id for role in getattr(interaction.user, "roles", [])]
        if not any(role_id in settings.allowed_roles for role_id in roles):
            return "You do not have permission to use operator commands."
    return None


def format_status_message(payload: dict[str, Any]) -> str:
    live_sync = dict(payload.get("live_exchange_sync") or {})
    workers = list(payload.get("worker_statuses") or [])
    alarms = list(payload.get("alarms") or [])
    gates = list(payload.get("acceptance_gates") or [])
    execution_quality = dict(payload.get("execution_quality") or {})
    gate_summary = ", ".join(
        f"{gate.get('gate')}:{gate.get('status')}" for gate in gates[:5]
    ) or "unknown"
    failing_workers = [
        str(worker.get("service"))
        for worker in workers
        if str(worker.get("status")) not in {"ok", "running"}
    ]
    return "\n".join(
        [
            "Stack: `kalshi-v3`",
            f"Exchange: `{payload.get('exchange', 'unknown')}`",
            f"Trading: paper `{payload.get('paper_trading_enabled', False)}` | live `{payload.get('live_trading_enabled', False)}` | placement `{payload.get('live_order_placement_enabled', False)}`",
            f"Feed ages: market `{payload.get('market_feed_age_seconds', 'n/a')}`s | reference `{payload.get('reference_feed_age_seconds', 'n/a')}`s | live_sync `{payload.get('live_exchange_sync_age_seconds', 'n/a')}`s",
            f"Live sync: positions `{live_sync.get('positions_count', 0)}` | orders `{live_sync.get('resting_orders_count', 0)}` | fills `{live_sync.get('recent_fills_count', 0)}`",
            f"Execution truth: live_sample `{execution_quality.get('live_sample_sufficient', False)}` | replay_sample `{execution_quality.get('replay_sample_sufficient', False)}` | live_terminal_intents `{execution_quality.get('recent_live_terminal_intent_count', 0)}`",
            f"Gates: {gate_summary}",
            f"Workers: {'all healthy' if not failing_workers else ', '.join(failing_workers)}",
            f"Alarms: {'none' if not alarms else len(alarms)}",
        ]
    )


def format_loop_status() -> str:
    codex_result = load_json(CODEX_RESULT_FILE, default={}) or {}
    reviewer_result = load_json(REVIEWER_RESULT_FILE, default={}) or {}
    needs_input = NEEDS_INPUT_FILE.exists()
    running_pids = running_loop_processes()
    status = "running" if running_pids else str(codex_result.get("status") or "unknown")
    files_changed = len(codex_result.get("files_changed") or [])
    verification = codex_result.get("verification") or []
    reviewer_approved = reviewer_result.get("approved")
    critical_count = len(reviewer_result.get("critical_trade_blockers") or [])
    lines = [
        "Loop: `kalshi-v3/.ai-loop`",
        f"Current run: `{status}`",
        f"Running PID: `{', '.join(str(pid) for pid in running_pids) if running_pids else (current_loop_pid() or 'none')}`",
        "",
        "Last completed result:",
        f"- Codex status: `{codex_result.get('status') or 'unknown'}`",
        f"- Files changed: `{files_changed}`",
        f"- Verification: `{verification[0] if verification else 'none'}`",
        f"- Reviewer approved: `{reviewer_approved if reviewer_approved is not None else 'n/a'}`",
        f"- Critical blockers: `{critical_count}`",
        f"- Needs input: `{needs_input}`",
    ]
    return "\n".join(lines)


def write_operator_input(*, approved: bool, stop: bool, notes: str = "") -> None:
    current = load_json(
        OPERATOR_INPUT_FILE,
        default={"approved": True, "notes": "", "scope_adjustments": [], "stop": False},
    )
    payload = {
        "approved": approved,
        "notes": notes or str(current.get("notes", "")),
        "scope_adjustments": list(current.get("scope_adjustments", [])),
        "stop": stop,
    }
    write_json(OPERATOR_INPUT_FILE, payload)


def read_pid_file(path: pathlib.Path) -> int | None:
    if not path.exists():
        return None
    raw = path.read_text(encoding="utf-8").strip()
    if not raw.isdigit():
        return None
    return int(raw)


def process_running(pid: int) -> bool:
    if pid <= 0:
        return False
    result = subprocess.run(
        ["tasklist", "/FI", f"PID eq {pid}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return str(pid) in result.stdout


def current_loop_pid() -> int | None:
    pid = read_pid_file(RUN_LOOP_PID_FILE)
    if pid is None:
        return None
    if process_running(pid):
        return pid
    try:
        RUN_LOOP_PID_FILE.unlink()
    except FileNotFoundError:
        pass
    return None


def running_loop_processes() -> list[int]:
    try:
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                r"Get-CimInstance Win32_Process | Where-Object { ($_.Name -eq 'python.exe' -or $_.Name -eq 'cmd.exe') -and ($_.CommandLine -match 'kalshi-v3\\\.ai-loop\\run_loop.py|nvm4w\\nodejs\\codex\.CMD|codex exec') } | Select-Object -ExpandProperty ProcessId",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return []
    pids: list[int] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.isdigit():
            pids.append(int(line))
    return pids


def launch_loop() -> int:
    ROOT.mkdir(parents=True, exist_ok=True)
    (ROOT / "logs").mkdir(parents=True, exist_ok=True)
    stdout_handle = RUN_LOOP_STDOUT_FILE.open("w", encoding="utf-8")
    stderr_handle = RUN_LOOP_STDERR_FILE.open("w", encoding="utf-8")
    creationflags = 0
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(
            subprocess,
            "DETACHED_PROCESS",
            0,
        )
    process = subprocess.Popen(
        [sys.executable, str(ROOT / "run_loop.py")],
        cwd=str(REPO_ROOT),
        stdout=stdout_handle,
        stderr=stderr_handle,
        creationflags=creationflags,
        close_fds=False,
    )
    RUN_LOOP_PID_FILE.write_text(str(process.pid), encoding="utf-8")
    return process.pid


class OperatorClient(discord.Client):
    def __init__(self, settings: Settings) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        super().__init__(intents=intents, allowed_mentions=discord.AllowedMentions.none())
        self.settings = settings
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self) -> None:
        guild = discord.Object(id=self.settings.guild_id)
        group = app_commands.Group(name="ai", description="Operate kalshi-v3")

        async def run_authorized_command(
            interaction: discord.Interaction,
            handler: Any,
        ) -> None:
            error = authorization_error(self.settings, interaction)
            if error:
                await interaction.response.send_message(error, ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True, thinking=True)
            try:
                content = handler()
                if asyncio.iscoroutine(content):
                    content = await content
            except urllib.error.URLError as exc:
                content = f"Failed to reach kalshi-v3 API: `{exc.reason}`"
            except Exception as exc:  # noqa: BLE001
                content = f"Command failed: `{exc}`"
            await interaction.followup.send(str(content), ephemeral=True)

        @group.command(name="status", description="Show kalshi-v3 runtime status")
        async def status(interaction: discord.Interaction) -> None:
            await run_authorized_command(
                interaction,
                lambda: format_status_message(fetch_json(f"{self.settings.api_base}/v1/runtime")),
            )

        @group.command(name="loop_status", description="Show local .ai-loop status")
        async def loop_status(interaction: discord.Interaction) -> None:
            await run_authorized_command(interaction, format_loop_status)

        @group.command(name="approve_loop", description="Approve the next .ai-loop pass")
        @app_commands.describe(note="Optional operator guidance for the next loop pass")
        async def approve_loop(
            interaction: discord.Interaction,
            note: str | None = None,
        ) -> None:
            await run_authorized_command(
                interaction,
                lambda: (
                    write_operator_input(approved=True, stop=False, notes=note or ""),
                    "Wrote `.ai-loop/operator_input.json` with `approved=true`.",
                )[1],
            )

        @group.command(name="run_loop", description="Launch the local .ai-loop via Codex CLI")
        @app_commands.describe(note="Optional operator guidance for this loop run")
        async def run_loop(
            interaction: discord.Interaction,
            note: str | None = None,
        ) -> None:
            async def handler() -> str:
                if note:
                    write_operator_input(approved=True, stop=False, notes=note)
                existing_pid = current_loop_pid()
                if existing_pid is not None:
                    return f"Loop already running with PID `{existing_pid}`."
                pid = launch_loop()
                return (
                    f"Started `.ai-loop/run_loop.py` with PID `{pid}`.\n"
                    f"Logs: `{RUN_LOOP_STDOUT_FILE.name}` / `{RUN_LOOP_STDERR_FILE.name}`"
                )

            await run_authorized_command(interaction, handler)

        @group.command(name="stop_loop", description="Stop the local .ai-loop")
        async def stop_loop(interaction: discord.Interaction) -> None:
            await run_authorized_command(
                interaction,
                lambda: (
                    write_operator_input(approved=False, stop=True),
                    "Wrote `.ai-loop/operator_input.json` with `stop=true`.",
                )[1],
            )

        self.tree.add_command(group, guild=guild)
        await self.tree.sync(guild=guild)


async def main() -> None:
    settings = load_settings()
    client = OperatorClient(settings)
    await client.start(settings.token)


if __name__ == "__main__":
    asyncio.run(main())
