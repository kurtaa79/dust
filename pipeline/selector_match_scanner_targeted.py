
import os
import re
import uuid
from datetime import datetime, timezone
import pandas as pd

# ---------- Config (expanded by default) ----------
INPUT_FILE = os.getenv("ABI_SIG_CSV", "results/abi_signatures_targeted.csv")
OUTPUT_FILE = os.getenv("SELECTOR_MATCHES_OUT", "results/selector_matches_targeted.csv")

# Permanently use expanded keywords by default
EXPANDED_KEYWORDS = [
    # rewards / claims
    "claim", "claimall", "claimrewards", "getreward", "getrewards", "collect",
    "harvest", "compound", "restake", "redeem",
    # moving funds
    "withdraw", "withdrawall", "unstake", "unstakeall", "exit", "cashout",
    "payout", "distribute", "allocate", "release", "unlock",
    # inflows / issuance
    "mint", "airdrop", "reward", "bonus", "dividend",
    # staking
    "stake", "deposit", "bond", "rebond",
    # misc ops
    "rebase", "wrap", "unwrap", "migrate", "rescue", "sweep",
]

# Allow override via env if you ever want to customize
kw_env = os.getenv("TARGET_KEYWORDS", "").strip()
if kw_env:
    KEYWORDS = [k.strip() for k in kw_env.split(",") if k.strip()]
else:
    # Optional: append more keywords via APPEND_KEYWORDS
    APPEND_KEYWORDS = [k.strip() for k in os.getenv("APPEND_KEYWORDS", "").split(",") if k.strip()]
    KEYWORDS = EXPANDED_KEYWORDS + APPEND_KEYWORDS

# Optional extras kept behind flags (off by default to preserve schema/console)
ADD_PROVENANCE = os.getenv("SELECTOR_ADD_PROVENANCE", "0") == "1"
SHOW_PREVIEW = os.getenv("SELECTOR_SHOW_PREVIEW", "0") == "1"
PREVIEW_ROWS = int(os.getenv("SELECTOR_PREVIEW_ROWS", "10"))

# ---------- Load ----------
if not os.path.exists(INPUT_FILE):
    raise SystemExit(f"❌ Missing input: {INPUT_FILE}")

df = pd.read_csv(INPUT_FILE)

if "Function" not in df.columns:
    # Try alias
    lower_map = {c.lower(): c for c in df.columns}
    if "abi_signature" in lower_map:
        df = df.rename(columns={lower_map["abi_signature"]: "Function"})
    else:
        raise SystemExit("❌ 'Function' column missing in ABI CSV.")

if "Address" not in df.columns:
    # Try case-insensitive recover
    for c in df.columns:
        if c.lower() == "address":
            df = df.rename(columns={c: "Address"})
            break
if "Address" not in df.columns:
    raise SystemExit("❌ 'Address' column missing.")

# ---------- Match (case-insensitive substring) ----------
func_lower = df["Function"].astype(str).str.lower()

mask_any = pd.Series(False, index=df.index)
matched_kw_series = pd.Series("", index=df.index, dtype=object)

for k in KEYWORDS:
    pat = re.escape(k.lower())
    hit = func_lower.str.contains(pat, regex=True, na=False)
    newly = hit & (~mask_any)
    matched_kw_series.loc[newly] = k
    mask_any = mask_any | hit

matches = df[mask_any].copy()

# ---------- Optional extras ----------
if ADD_PROVENANCE:
    if "MatchedKeyword" not in matches.columns:
        matches["MatchedKeyword"] = matched_kw_series.loc[matches.index].values
    if "SourceCSV" not in matches.columns:
        matches["SourceCSV"] = INPUT_FILE
    if "ScannedAt" not in matches.columns:
        matches["ScannedAt"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if "RunID" not in matches.columns:
        matches["RunID"] = uuid.uuid4().hex[:8]

# ---------- Save ----------
matches.to_csv(OUTPUT_FILE, index=False)

# ---------- Console output (unchanged style) ----------
print(f"✅ Found {len(matches)} matching functions using keywords: {KEYWORDS}")
print(f"🔽 Output: {OUTPUT_FILE}")

if SHOW_PREVIEW and len(matches) > 0:
    try:
        from tabulate import tabulate
        print("— Preview —")
        print(tabulate(matches.head(PREVIEW_ROWS), headers="keys", tablefmt="plain", showindex=False))
    except Exception:
        print("— Preview —")
        print(matches.head(PREVIEW_ROWS).to_string(index=False))
