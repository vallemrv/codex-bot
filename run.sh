#!/usr/bin/env bash
# Arranca codex-bot usando el entorno virtual local.
set -euo pipefail
cd "$(dirname "$0")"
exec .venv/bin/python src/telegram_bot.py
