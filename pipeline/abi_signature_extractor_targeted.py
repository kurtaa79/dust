import os
import time
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Any

# ------------------------
# Settings
# ------------------------
INPUT_FILE = "results/dust_enriched_results_targeted.csv"
OUTPUT_FILE = "results/abi_signatures_targeted.csv"
CACHE_FILE = "results/abi_cache.json"   # avoids refetching
RATE_LIMIT_PER_SEC = 4                  # Etherscan allows ~5/s; stay under
RETRY_COUNT = 3
RETRY_DELAY = 1.5

# ------------------------
# Env & API
# ------------------------
load_dotenv()
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
if not ETHERSCAN_API_KEY:
    raise RuntimeError("ETHERSCAN_API_KEY not set in .env")

ETHERSCAN_API = "https://api.etherscan.io/api"

# ------------------------
# Helpers
# ------------------------
def load_cache(path: Path) -> Dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}

def save_cache(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(path)

def fetch_abi(address: str, session: requests.Session) -> Any:
    """Return parsed ABI (list) or None."""
    params = {
        "module": "contract",
        "action": "getabi",
        "address": address,
        "apikey": ETHERSCAN_API_KEY,
    }
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = session.get(ETHERSCAN_API, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "1" and data.get("result"):
                try:
                    return json.loads(data["result"])
                except Exception:
                    # Sometimes result is already a list
                    if isinstance(data["result"], list):
                        return data["result"]
            # Non-verified contracts or errors return status "0"
            return None
        except requests.RequestException:
            if attempt == RETRY_COUNT:
                return None
            time.sleep(RETRY_DELAY)
    return None

def extract_functions(abi: Any) -> List[str]:
    names = []
    if isinstance(abi, list):
        for entry in abi:
            if isinstance(entry, dict) and entry.get("type") == "function":
                name = entry.get("name")
                if name:
                    names.append(name)
    return names

# ------------------------
# Main
# ------------------------
def main():
    print(f"🔍 Reading: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    if "Address" not in df.columns:
        raise SystemExit("❌ 'Address' column missing in enriched CSV.")
    # Normalize address casing
    df["Address"] = df["Address"].astype(str).str.strip()

    # Load ABI cache
    cache_path = Path(CACHE_FILE)
    cache = load_cache(cache_path)

    session = requests.Session()
    out_rows = []
    unique_addrs = sorted(set(df["Address"].tolist()))
    print(f"🧩 Unique contracts to query: {len(unique_addrs)}")

    last_ts = 0.0
    for i, addr in enumerate(unique_addrs, 1):
        addr_lc = addr.lower()

        if addr_lc in cache:
            abi = cache[addr_lc]
        else:
            # Rate limit
            elapsed = time.time() - last_ts
            if elapsed < (1.0 / RATE_LIMIT_PER_SEC):
                time.sleep((1.0 / RATE_LIMIT_PER_SEC) - elapsed)
            abi = fetch_abi(addr_lc, session)
            cache[addr_lc] = abi  # store even if None to avoid retries in same run
            last_ts = time.time()
            if i % 25 == 0:
                save_cache(cache_path, cache)

        funcs = extract_functions(abi) if abi else []
        if not funcs:
            # Ensure at least one row so downstream sees the address
            out_rows.append({"Address": addr, "Function": ""})
        else:
            for fn in funcs:
                out_rows.append({"Address": addr, "Function": fn})

        if i % 50 == 0:
            print(f"  ↳ Processed {i}/{len(unique_addrs)}")

    # Final cache save
    save_cache(cache_path, cache)

    # Create DataFrame of (Address, Function) and (optionally) merge back metadata
    func_df = pd.DataFrame(out_rows)
    # Keep some metadata columns if present
    keep_cols = [c for c in ["Type", "Block", "extra_info", "Balance (ETH)"] if c in df.columns]
    meta_df = df[["Address"] + keep_cols].drop_duplicates("Address")
    merged = func_df.merge(meta_df, on="Address", how="left")

    # Write out
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ ABI signatures saved to: {OUTPUT_FILE} (rows: {len(merged)})")

if __name__ == "__main__":
    main()
