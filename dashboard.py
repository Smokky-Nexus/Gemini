"""
dashboard.py — Live Status Dashboard
======================================
Renders real-time statistics for Beast Engine v8.
"""
from __future__ import annotations
import sys
import time
from datetime import timedelta
from typing import Any

_RICH = False
try:
    from rich.live import Live
    from rich.table import Table
    from rich.console import Console
    from rich.panel import Panel as RichPanel
    _console = Console()
    _RICH = True
except ImportError:
    _console = None

def format_runtime(start_time: float) -> str:
    elapsed = int(time.time() - start_time)
    return str(timedelta(seconds=elapsed))

def build_dashboard_table(stats: dict[str, Any]) -> Any:
    if not _RICH:
        return None

    runtime = format_runtime(stats.get("start_time", time.time()))

    table = Table(show_header=False, expand=True, box=None)
    table.add_column("Col1", justify="left")
    table.add_column("Col2", justify="left")

    c1 = (
        f"[bold cyan]⏱  Runtime[/]      {runtime}\n"
        f"[bold yellow]🔍 Probed[/]       {stats.get('panels_probed', 0):,}\n"
        f"[bold green]📡 Live Panels[/]   {stats.get('panels_live', 0):,}\n"
        f"[bold blue]📱 Devices[/]      {stats.get('devices_online', 0):,}\n"
        f"[bold magenta]📞 Numbers[/]      {stats.get('numbers_found', 0):,}\n"
        f"[bold white]📨 OTP Sent[/]     {stats.get('otps_sent', 0):,}\n"
        f"[bold bright_green]✅ Verified[/]     {stats.get('otps_verified', 0):,}\n"
        f"[bold gold1]💎 CLAIMED[/]      {stats.get('links_claimed', 0):,}"
    )

    c2 = (
        f"[bold cyan]⚡ Engine[/]        ASYNC v8 PIPELINE\n"
        f"[bold red]📵 Not Jio[/]       {stats.get('not_jio', 0):,}\n"
        f"[bold yellow]🔒 Locked[/]        {stats.get('locked', 0):,}\n"
        f"[bold dim]💀 Dead Dev[/]      {stats.get('dead_device', 0):,}\n"
        f"[bold orange3]⚠️  Send Fail[/]     {stats.get('otp_send_fail', 0):,}\n"
        f"[bold bright_blue]🔄 OTP Active[/]    {stats.get('otp_active_polling', 0):,}\n"
        f"[bold dim]⏳ OTP Miss[/]      {stats.get('otp_timeout', 0):,}\n"
        f"[bold green]✅ Fresh[/]         {stats.get('links_fresh', 0):,}\n"
        f"[bold white]📥 Q:Nums[/]       {stats.get('q_numbers', 0):,}"
    )

    table.add_row(c1, c2)

    return RichPanel(
        table,
        title="[bold red]🔥 BEAST ENGINE v8 — INSTANT ASYNC PIPELINE 🔥[/]",
        border_style="red"
    )

def print_fallback_status(stats: dict[str, Any]) -> None:
    runtime = format_runtime(stats.get("start_time", time.time()))
    print(
        f"[{runtime}] Live: {stats.get('panels_live',0)} | Nums: {stats.get('numbers_found',0)} | "
        f"Sent: {stats.get('otps_sent',0)} | Not Jio: {stats.get('not_jio',0)} | Locked: {stats.get('locked',0)} | "
        f"Dead: {stats.get('dead_device',0)} | "
        f"Send Fail: {stats.get('otp_send_fail',0)} | Active OTP: {stats.get('otp_active_polling',0)} | "
        f"Verified: {stats.get('otps_verified',0)} | Miss: {stats.get('otp_timeout',0)} | "
        f"Claimed: {stats.get('links_claimed',0)}"
    )
