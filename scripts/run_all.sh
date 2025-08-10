#!/usr/bin/env bash
set -euo pipefail

# Root of your project (edit if needed)
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

mkdir -p "$ROOT_DIR/logs" "$ROOT_DIR/state" "$ROOT_DIR/results"

echo "▶️  Activating venv..."
source "$ROOT_DIR/venv/bin/activate"

echo "🚀 Running sniper... (logs/sniper_$(date +%F).log)"
python "$ROOT_DIR/scripts/dust_sniper_state.py" | tee -a "$ROOT_DIR/logs/sniper_$(date +%F).log"

echo "✅ Done."
