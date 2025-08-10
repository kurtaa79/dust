import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

# -------- Config --------
CALL_RESULTS_CSV = os.getenv("CALL_RESULTS_CSV", "results/call_builder_results.csv")
ABI_CACHE_PATH = Path(os.getenv("ABI_CACHE_PATH", "results/abi_cache.json"))
OUT_CSV = os.getenv("PREFLIGHT_OUT", "results/preflight_claimables.csv")

load_dotenv()
RPC = os.getenv("WEB3_PROVIDER_URL") or os.getenv("WEB3_PROVIDER") or os.getenv("ETH_RPC_URL")
FROM_ADDRESS = os.getenv("FROM_ADDRESS")
if not RPC or not FROM_ADDRESS:
    raise SystemExit("❌ Need WEB3_PROVIDER_URL and FROM_ADDRESS in .env")

w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 30}))
from_addr = Web3.to_checksum_address(FROM_ADDRESS)

CANDIDATE_NAMES = [
    "claimable", "claimables", "claimableAmount", "claimableRewards",
    "pending", "pendingReward", "pendingRewards", "pendingrewards", "pending_amount",
    "releasable", "releasableAmount", "withdrawable", "withdrawableAmount",
    "available", "availableToWithdraw", "availableRewards",
    "earned", "rewards", "reward", "accrued", "accruedRewards",
    "profit", "balanceToWithdraw",
]

def load_cache(path: Path) -> Dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}

def find_candidate_funcs(abi: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for e in abi:
        if not isinstance(e, dict):
            continue
        if e.get("type") != "function":
            continue
        mut = (e.get("stateMutability") or "").lower()
        if mut not in ("view", "pure"):
            continue
        name = (e.get("name") or "").strip()
        lname = name.lower()
        if lname in [c.lower() for c in CANDIDATE_NAMES]:
            out.append(e)
        else:
            for kw in ["claim", "pend", "reward", "withdraw", "release", "releas", "accru", "avail", "earn"]:
                if kw in lname:
                    out.append(e)
                    break
    return out

def call_func(contract, fn_abi, from_addr) -> Tuple[int, str]:
    # Try to call the function. Supports 0-arg and 1-arg (address).
    name = fn_abi.get("name")
    inputs = fn_abi.get("inputs") or []
    try:
        if len(inputs) == 0:
            fn = contract.get_function_by_signature(f"{name}()")()
            ret = fn.call({"from": from_addr})
        elif len(inputs) == 1 and (inputs[0].get("type") in ("address","address payable")):
            fn = contract.get_function_by_signature(f"{name}(address)")(from_addr)
            ret = fn.call({"from": from_addr})
        else:
            return 0, "skip:requires_params"
        if isinstance(ret, int):
            return int(ret), ""
        if isinstance(ret, (list, tuple)) and ret:
            for v in ret:
                if isinstance(v, int):
                    return int(v), "tuple_pick_first_int"
        return 0, "non_numeric"
    except Exception as e:
        return 0, f"error:{e.__class__.__name__}"

def main():
    if not Path(CALL_RESULTS_CSV).exists():
        raise SystemExit(f"Missing {CALL_RESULTS_CSV}")
    df = pd.read_csv(CALL_RESULTS_CSV)
    if "Address" not in df.columns:
        raise SystemExit("CSV must contain Address column")

    addrs = sorted(set(str(a).strip() for a in df["Address"].dropna().tolist()))
    cache = load_cache(ABI_CACHE_PATH)

    rows = []
    for i, addr in enumerate(addrs, 1):
        key = addr.lower()
        abi = cache.get(key)
        if not abi:
            continue
        ctr = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=abi)
        funcs = find_candidate_funcs(abi)
        for fn_abi in funcs:
            raw, note = call_func(ctr, fn_abi, from_addr)
            if raw > 0 or note.startswith("error"):
                rows.append({
                    "Address": addr,
                    "Function": fn_abi.get("name"),
                    "RawValue": raw,
                    "AsEther": raw / 1e18,
                    "Note": note
                })
        if i % 50 == 0:
            print(f"  ↳ Scanned {i}/{len(addrs)} contracts")

    out = pd.DataFrame(rows).sort_values(["AsEther","RawValue"], ascending=[False,False])
    Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"✅ Preflight done → {OUT_CSV} (rows: {len(out)})")
    if len(out) > 0:
        print(out.head(15).to_string(index=False))

if __name__ == "__main__":
    main()
