#!/usr/bin/env bash
set -euo pipefail
# Launch Dust sniper (one pass from last_block to head), verbose
# Place this file in your Dust project root (same folder that contains 'sniper', 'results', 'state', 'venv')
cd "$(dirname "$0")"

# Activate venv if present
if [ -d "venv" ]; then source "venv/bin/activate"; fi

# ---- REQUIRED: set your RPC (Alchemy/Infura/etc.) ----
: "${WEB3_PROVIDER:?Set WEB3_PROVIDER in your environment or edit this file to include it}"

# Optional tuning (edit if you want)
export DUST_MIN="${DUST_MIN:-1e-5}"
export DUST_MAX="${DUST_MAX:-0.01}"
export WORKERS="${WORKERS:-8}"
export FOLLOW=0
export LOG_EVERY="${LOG_EVERY:-10}"

python3 sniper/dust_sniper_stateful_verbose.py
read -p $'\nDone. Press Enter to close...'
