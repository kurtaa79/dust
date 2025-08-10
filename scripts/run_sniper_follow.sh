#!/usr/bin/env bash
set -euo pipefail
# Launch Dust sniper in follow mode (tail new blocks forever)
cd "$(dirname "$0")"

if [ -d "venv" ]; then source "venv/bin/activate"; fi

: "${WEB3_PROVIDER:?Set WEB3_PROVIDER in your environment or edit this file to include it}"

export DUST_MIN="${DUST_MIN:-1e-5}"
export DUST_MAX="${DUST_MAX:-0.01}"
export WORKERS="${WORKERS:-8}"
export FOLLOW=1
export LOG_EVERY="${LOG_EVERY:-10}"

python3 sniper/dust_sniper_stateful_verbose.py
