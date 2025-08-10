
import pandas as pd
from pathlib import Path

INPUT = Path("results/dust_sniper_results_v2.csv")
OUTPUT = Path("results/high_value_contract_targets.csv")

if not INPUT.exists():
    raise SystemExit(f"❌ Missing input CSV: {INPUT}")

# Read robustly: tolerate malformed lines
df = pd.read_csv(INPUT, engine="python", on_bad_lines="skip")

# Normalize column names (trim whitespace)
df.rename(columns={c: c.strip() for c in df.columns}, inplace=True)

# Find balance column
balance_col = next((c for c in ["Balance (ETH)", "BalanceETH", "balance_eth", "Balance"] if c in df.columns), None)
if not balance_col:
    raise SystemExit(f"❌ Could not find a balance column in: {list(df.columns)}")

# Coerce balance to numeric
df[balance_col] = pd.to_numeric(df[balance_col], errors="coerce")

# Address column
addr_col = next((c for c in ["Address", "ChecksumAddress", "address"] if c in df.columns), None)
if not addr_col:
    raise SystemExit(f"❌ Could not find an address column in: {list(df.columns)}")

# Type column (Contract/Wallet)
type_col = next((c for c in ["Type", "type"] if c in df.columns), None)
if type_col is None:
    # default to Wallet if absent
    df["Type"] = "Wallet"
    type_col = "Type"

before = len(df)
filtered = df[
    (df[type_col].astype(str).str.lower() == "contract") &
    (df[balance_col] > 0)
].copy()

# Keep a tidy subset of useful columns if present
keep_cols = [col for col in [addr_col, "ChecksumAddress", balance_col, type_col, "Block", "TimestampUTC"] if col in df.columns]
if keep_cols:
    filtered = filtered[keep_cols]

filtered = filtered.sort_values(by=balance_col, ascending=False)

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
filtered.to_csv(OUTPUT, index=False)

print(f"✅ Done. {len(filtered)} high-value contracts saved to: {OUTPUT}")
print(f"ℹ️ Parsed rows: {before} → kept: {len(filtered)} (bad lines were skipped).")
