import os
import re
import pandas as pd
from pathlib import Path

# Inputs (can override via env if you want)
CALLS_CSV = os.getenv("CALL_BUILDER_RESULTS", "results/call_builder_results.csv")
ENRICHED_CSV = os.getenv("ENRICHED_RESULTS", "results/dust_enriched_results_targeted.csv")
OUT_CSV = os.getenv("SHORTLIST_OUT", "results/call_builder_shortlist.csv")

# Tunables
MIN_BAL_ETH = float(os.getenv("MIN_BAL_ETH", "0.01"))   # ignore dust-balance contracts
GAS_MIN = int(os.getenv("GAS_MIN", "25000"))            # too low often means revert/no-op
GAS_MAX = int(os.getenv("GAS_MAX", "400000"))           # >400k usually not simple withdraws
SCORE_THRESHOLD = int(os.getenv("SCORE_THRESHOLD", "4"))

# Keywords (strong signals)
KEYWORDS = [k.strip().lower() for k in os.getenv("FILTER_KEYWORDS",
    "claim,withdraw,airdrop,harvest,collect,redeem,unstake,stake,mint,distribute,release,unlock,bonus,dividend"
).split(",") if k.strip()]

def main():
    if not Path(CALLS_CSV).exists():
        raise SystemExit(f"Missing {CALLS_CSV}")
    calls = pd.read_csv(CALLS_CSV)

    # Merge balance info if present
    if Path(ENRICHED_CSV).exists():
        enriched = pd.read_csv(ENRICHED_CSV)
        if "Address" in enriched.columns and "Balance (ETH)" in enriched.columns:
            bal = enriched[["Address", "Balance (ETH)"]].drop_duplicates("Address")
            calls = calls.merge(bal, on="Address", how="left")

    # Normalize
    calls["Function_str"] = calls["Function"].astype(str)
    calls["fn_lower"] = calls["Function_str"].str.lower()
    calls["Notes"] = calls.get("Notes", "").astype(str)

    # Signals
    calls["sig_keyword"] = False
    for kw in KEYWORDS:
        calls["sig_keyword"] = calls["sig_keyword"] | calls["fn_lower"].str.contains(re.escape(kw), na=False)

    calls["sig_est_ok"] = calls.get("EstimateOK", False) == True
    calls["sig_call_ok"] = calls.get("CallOK", False) == True
    calls["sig_gas_band"] = calls.get("GasEstimate", 0).fillna(0).between(GAS_MIN, GAS_MAX, inclusive="both")
    calls["sig_nonview"] = ~calls.get("Mutability","").astype(str).str.lower().isin(["view","pure"])

    # Penalties
    calls["pen_args"] = calls["Notes"].str.contains("requires_args_or_not_found", na=False)
    calls["pen_noabi"] = calls["Notes"].str.contains("no_abi", na=False)

    # Balance filter
    if "Balance (ETH)" in calls.columns:
        calls["sig_balance"] = calls["Balance (ETH)"].fillna(0) >= MIN_BAL_ETH
    else:
        calls["sig_balance"] = True  # if we don't know, don't block

    # Scoring (tweakable weights)
    score = (
        2 * calls["sig_est_ok"].astype(int) +
        2 * calls["sig_gas_band"].astype(int) +
        3 * calls["sig_call_ok"].astype(int) +
        1 * calls["sig_keyword"].astype(int) +
        1 * calls["sig_nonview"].astype(int) +
        1 * calls["sig_balance"].astype(int) -
        2 * calls["pen_args"].astype(int) -
        1 * calls["pen_noabi"].astype(int)
    )
    calls["ConfidenceScore"] = score

    # Shortlist
    shortlist = calls.sort_values(["ConfidenceScore","GasEstimate"], ascending=[False,True])
    shortlist = shortlist[shortlist["ConfidenceScore"] >= SCORE_THRESHOLD]

    # Keep useful columns
    keep_cols = [c for c in [
        "Address","Function","Mutability","CallOK","EstimateOK","GasEstimate","Balance (ETH)","Notes","ConfidenceScore"
    ] if c in shortlist.columns]
    shortlist = shortlist[keep_cols].drop_duplicates()

    Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    shortlist.to_csv(OUT_CSV, index=False)

    print(f"✅ Wrote shortlist → {OUT_CSV} (rows: {len(shortlist)})")
    if len(shortlist) > 0:
        print('— Preview —')
        print(shortlist.head(15).to_string(index=False))

if __name__ == "__main__":
    main()
