import os
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

# Web3
from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.middleware.proof_of_authority import ExtraDataToPOAMiddleware as geth_poa_middleware

# --- Patch: helper to find single-address-arg function variant ---
def _find_single_address_fn(abi_list, fn_name: str):
    try:
        for item in (abi_list or []):
            if not isinstance(item, dict): 
                continue
            if item.get("type") != "function":
                continue
            if item.get("name") != fn_name:
                continue
            inputs = item.get("inputs") or []
            if len(inputs) == 1 and str(inputs[0].get("type","")).lower() == "address":
                return item
    except Exception:
        pass
    return None


# --------------- Config ---------------
MATCHES_CSV = os.getenv("SELECTOR_MATCHES_CSV", "results/selector_matches_targeted.csv")
ABI_CACHE_PATH = Path(os.getenv("ABI_CACHE_PATH", "results/abi_cache.json"))
OUTPUT_CSV = os.getenv("CALL_BUILDER_OUT", "results/call_builder_results.csv")

# Network
load_dotenv()
RPC_URL = os.getenv("WEB3_PROVIDER_URL") or os.getenv("ALCHEMY_HTTP") or os.getenv("ALCHEMY_URL")
if not RPC_URL:
    raise SystemExit("❌ Set WEB3_PROVIDER_URL (or ALCHEMY_HTTP/ALCHEMY_URL) in .env")

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")  # optional; used as fallback if cache is missing

# Address to simulate calls from (important for eligibility checks)
FROM_ADDRESS = os.getenv("FROM_ADDRESS")
if not FROM_ADDRESS:
    # If not provided, try infer from PRIVATE_KEY (never printed)
    pk = os.getenv("PRIVATE_KEY")
    if pk:
        from eth_account import Account
        FROM_ADDRESS = Account.from_key(pk).address
    else:
        # last resort: use zero address (suboptimal for msg.sender-based checks)
        FROM_ADDRESS = "0x0000000000000000000000000000000000000000"

# Limits
RATE_LIMIT_PER_SEC = float(os.getenv("ETHERSCAN_RPS", "4"))
RETRY_COUNT = 3
RETRY_DELAY = 1.5

# --------------- Web3 init ---------------
w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 30}))
# Add POA middleware just in case (harmless on mainnet)
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

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

def fetch_abi_from_etherscan(address: str) -> Optional[List[Dict[str, Any]]]:
    """Fetch ABI for address using Etherscan, if key is available."""
    if not ETHERSCAN_API_KEY:
        return None
    import requests
    params = {
        "module": "contract",
        "action": "getabi",
        "address": address,
        "apikey": ETHERSCAN_API_KEY,
    }
    url = "https://api.etherscan.io/api"
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "1" and data.get("result"):
                try:
                    return json.loads(data["result"])
                except Exception:
                    if isinstance(data["result"], list):
                        return data["result"]
            return None
        except Exception:
            if attempt == RETRY_COUNT:
                return None
            time.sleep(RETRY_DELAY)
    return None

def get_abi(address: str, cache: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    key = address.lower()
    if key in cache:
        return cache[key]
    abi = fetch_abi_from_etherscan(key)
    cache[key] = abi
    return abi

def list_zero_arg_functions(abi: List[Dict[str, Any]], name: str) -> List[Dict[str, Any]]:
    """Return ABI entries for functions matching the given name with zero inputs."""
    out = []
    for e in abi:
        if not isinstance(e, dict):
            continue
        if e.get("type") != "function":
            continue
        if e.get("name") != name:
            continue
        inputs = e.get("inputs") or []
        if len(inputs) == 0:
            out.append(e)
    return out

def try_eth_call_and_estimate(addr: str, fn_abi: Dict[str, Any]) -> Tuple[bool, str, bool, int, str]:
    """
    Attempt eth_call and gas estimation for a zero-arg function.
    Returns: (call_ok, call_ret, est_ok, gas_est, note)
    """
    contract = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=[fn_abi])
    # Build the function object by name (zero args)
    fn = contract.get_function_by_signature(f"{fn_abi['name']}()")()

    # eth_call (simulation) - even non-view can be simulated; may revert
    call_ok, call_ret, note = False, "", ""
    try:
        # Use from=FROM_ADDRESS so msg.sender checks are realistic
        ret = fn.call({"from": FROM_ADDRESS})
        call_ok = True
        # Represent return as string
        call_ret = str(ret)
    except ContractLogicError as e:
        note = f"revert: {e}"
    except Exception as e:
        note = f"call_error: {e.__class__.__name__}"

    # estimateGas
    est_ok, gas_est = False, 0
    try:
        tx = fn.build_transaction({"from": FROM_ADDRESS})
        gas_est = w3.eth.estimate_gas(tx)
        est_ok = True
    except Exception as e:
        # leave as False
        if note:
            note += f"; "
        note += f"est_error: {e.__class__.__name__}"

    return call_ok, call_ret, est_ok, int(gas_est) if est_ok else 0, note

def main():
    print(f"🔧 Call Builder starting | RPC={RPC_URL}")
    print(f"📥 Reading matches: {MATCHES_CSV}")
    if not Path(MATCHES_CSV).exists():
        raise SystemExit(f"❌ Missing input CSV: {MATCHES_CSV}")
    df = pd.read_csv(MATCHES_CSV)

    if "Address" not in df.columns or "Function" not in df.columns:
        raise SystemExit("❌ CSV must contain 'Address' and 'Function' columns.")

    # Unique (Address, Function) pairs
    pairs = df[["Address", "Function"]].dropna().drop_duplicates().values.tolist()
    print(f"🧩 Unique targets: {len(pairs)}")

    cache = load_cache(ABI_CACHE_PATH)
    out_rows = []

    for i, (addr, fn_name) in enumerate(pairs, 1):
        addr = str(addr).strip()
        fn_name = str(fn_name).strip()
        if not addr or not fn_name:
            continue

        # Load ABI from cache or etherscan
        abi = get_abi(addr, cache)
        if not abi:
            out_rows.append({
                "Address": addr,
                "Function": fn_name,
                "ABIInputs": 0,
                "Mutability": "",
                "CallOK": False,
                "CallReturn": "",
                "EstimateOK": False,
                "GasEstimate": 0,
                "Notes": "no_abi"
            })
            if i % 25 == 0:
                save_cache(ABI_CACHE_PATH, cache)
            continue

        # Find zero-arg variants of this function
        matches = list_zero_arg_functions(abi, fn_name)
        if not matches:
            out_rows.append({
                "Address": addr,
                "Function": fn_name,
                "ABIInputs": -1,
                "Mutability": "",
                "CallOK": False,
                "CallReturn": "",
                "EstimateOK": False,
                "GasEstimate": 0,
                "Notes": "requires_args_or_not_found"
            })
            if i % 25 == 0:
                save_cache(ABI_CACHE_PATH, cache)
            continue

        # Try each zero-arg overload (usually 1)
        for fn_abi in matches:
            mut = fn_abi.get("stateMutability", "")
            call_ok, call_ret, est_ok, gas_est, note = try_eth_call_and_estimate(addr, fn_abi)

            out_rows.append({
                "Address": addr,
                "Function": fn_name,
                "ABIInputs": 0,
                "Mutability": mut,
                "CallOK": call_ok,
                "CallReturn": call_ret,
                "EstimateOK": est_ok,
                "GasEstimate": gas_est,
                "Notes": note
            })

        if i % 20 == 0 or i == len(pairs):
            print(f"  ↳ Processed {i}/{len(pairs)} targets")

        # Periodically persist ABI cache
        if i % 25 == 0:
            save_cache(ABI_CACHE_PATH, cache)

    # Final cache save
    save_cache(ABI_CACHE_PATH, cache)

    # Save output
    out_df = pd.DataFrame(out_rows)
    Path(OUTPUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    out_df

# === Patch: attempt estimates for single-address-arg functions using MY_ADDRESS ===
try:
    _MY_ADDRESS = Web3.to_checksum_address(os.getenv("MY_ADDRESS","0x000000000000000000000000000000000000dead"))
    # Load inputs/outputs DataFrame if available in scope: df (call targets) -> name may differ. 
    # We'll operate on 'out_rows' just before writing results.
    if 'out_rows' in locals() and isinstance(out_rows, list):
        improved = 0
        # Build a quick address->ABI cache
        _abi_cache_path = Path("results/abi_cache.json")
        _abi_cache = {}
        if _abi_cache_path.exists():
            try:
                import json as _json
                _abi_cache = _json.loads(_abi_cache_path.read_text())
            except Exception:
                _abi_cache = {}
        for i,row in enumerate(out_rows):
            try:
                if int(row.get("ABIInputs", -1)) != 1:
                    continue
                if row.get("EstimateOK", False):
                    continue
                addr = Web3.to_checksum_address(str(row["Address"]).strip())
                fn_name = str(row["Function"]).strip()
                abi = _abi_cache.get(addr.lower())
                if not abi:
                    continue
                # Find matching 1-address function
                item = _find_single_address_fn(abi, fn_name)
                if not item:
                    continue
                # Build stub ABI and function
                w3 = Web3(Web3.HTTPProvider(os.getenv("RPC_URL") or os.getenv("WEB3_RPC") or os.getenv("RPC") or ""))
                if not w3.is_connected():
                    continue
                contract = w3.eth.contract(address=addr, abi=[item])
                fn = contract.get_function_by_signature(f"{fn_name}(address)")(_MY_ADDRESS)
                # estimate
                est = w3.eth.estimate_gas({"from": _MY_ADDRESS, "to": addr, "data": fn._encode_transaction_data()})
                row["CallOK"] = True
                row["EstimateOK"] = True
                row["GasEstimate"] = int(est)
                row["Notes"] = (row.get("Notes","") + "; addr1_estimated").strip("; ")
                improved += 1
            except Exception:
                # leave as-is if anything fails
                pass
        if improved:
            print(f"[call_builder patch] Estimated {improved} single-address functions.")
except Exception:
    pass

.to_csv(OUTPUT_CSV, index=False)

    # Summary
    ok_calls = (out_df["CallOK"] == True).sum() if not out_df.empty else 0
    ok_est   = (out_df["EstimateOK"] == True).sum() if not out_df.empty else 0
    print(f"✅ Call Builder done → {OUTPUT_CSV}")
    print(f"   • Successful eth_call: {ok_calls}")
    print(f"   • Successful gas estimates: {ok_est}")
    if not out_df.empty:
        print("— Preview —")
        print(out_df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
