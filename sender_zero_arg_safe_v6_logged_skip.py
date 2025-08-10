import os
import math
import csv
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

# ------------- Config -------------
CALL_RESULTS_CSV = os.getenv("CALL_RESULTS_CSV", "results/call_builder_results.csv")
LOG_FILE_JSONL = os.getenv("SENDER_LOG", "results/sender_log.txt")
LOG_FILE_CSV = os.getenv("SENDER_LOG_CSV", "results/send_attempts_log.csv")

# Safety: DRY RUN by default
ENABLE_SEND = os.getenv("ENABLE_SEND", "0") == "1"
MAX_TX = int(os.getenv("MAX_TX", "1"))
MIN_BAL_ETH = float(os.getenv("MIN_BAL_ETH", "0.0"))   # shortlist already filters usually
FORCE = os.getenv("FORCE", "0") == "1"                 # override skip logic

# Gas & fees
GAS_BUMP = float(os.getenv("GAS_BUMP", "1.15"))
GAS_CAP = int(os.getenv("GAS_CAP", "200000"))
PRIO_FEE_GWEI = float(os.getenv("PRIO_FEE_GWEI", "1.0"))

# ------------- Env & Web3 -------------
load_dotenv()
RPC = os.getenv("WEB3_PROVIDER_URL") or os.getenv("WEB3_PROVIDER") or os.getenv("ETH_RPC_URL")
FROM_ADDRESS = os.getenv("FROM_ADDRESS")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")

if not RPC:
    raise SystemExit("❌ Missing RPC (WEB3_PROVIDER_URL / WEB3_PROVIDER / ETH_RPC_URL)")
if not FROM_ADDRESS:
    raise SystemExit("❌ Missing FROM_ADDRESS in .env")
if ENABLE_SEND and not PRIVATE_KEY:
    raise SystemExit("❌ ENABLE_SEND=1 but no PRIVATE_KEY in .env")

w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 30}))
assert w3.is_connected(), f"Cannot connect to RPC {RPC}"
from_addr = Web3.to_checksum_address(FROM_ADDRESS)

def calc_fees(w3: Web3):
    blk = w3.eth.get_block("latest")
    base = blk.get("baseFeePerGas", 0) or 0
    prio = int(PRIO_FEE_GWEI * 1e9)
    max_fee = int(base * 2 + prio)
    return base, prio, max_fee

def gwei(n): return float(n) / 1e9

# ------------- CSV logger -------------
CSV_HEADERS = ["timestamp","address","function","gas_estimate","gas_limit",
               "base_fee_gwei","priority_fee_gwei","max_fee_gwei",
               "sent","tx_hash","status"]

def csv_log_row(row: dict):
    path = Path(LOG_FILE_CSV)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if write_header:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in CSV_HEADERS})

# ------------- Load tried set -------------
tried = set()
log_path = Path(LOG_FILE_CSV)
if log_path.exists():
    try:
        prev = pd.read_csv(log_path)
        if {"address","function"}.issubset(prev.columns):
            tried = {(a.lower(), f) for a, f in zip(prev["address"].astype(str), prev["function"].astype(str))}
    except Exception:
        tried = set()

# ------------- Load & filter candidates -------------
df = pd.read_csv(CALL_RESULTS_CSV)

# Basic safety filters
filt = (df["CallOK"] == True) & (df["EstimateOK"] == True)
if "ABIInputs" in df.columns:
    filt &= df["ABIInputs"].fillna(-1).astype(int).eq(0)
if "Mutability" in df.columns:
    filt &= ~df["Mutability"].astype(str).str.lower().isin(["view","pure"])
if "Balance (ETH)" in df.columns:
    filt &= df["Balance (ETH)"].fillna(0) >= MIN_BAL_ETH
filt &= df["Function"].notna() & ~df["Function"].astype(str).str.contains(r"\(")  # zero-arg by name

candidates = df[filt].copy()
# Prefer lower gas first
if "GasEstimate" in candidates.columns:
    candidates = candidates.sort_values("GasEstimate")

# Apply skip unless FORCE=1
if not FORCE and len(tried) > 0:
    m = ~[( (str(a).lower(), str(f)) in tried ) for a, f in zip(candidates["Address"], candidates["Function"])]
    candidates = candidates[m]

candidates = candidates.head(MAX_TX)

print(f"🚀 Sender starting | DRY_RUN={not ENABLE_SEND} | RPC={RPC} | FORCE={FORCE}")
print(f"📌 Candidates this run: {len(candidates)} (MAX_TX={MAX_TX})")
if candidates.empty:
    print("No eligible candidates found under current filters (or all already tried).")
    raise SystemExit(0)

# ------------- Build & (optionally) send -------------
nonce = w3.eth.get_transaction_count(from_addr)
sent = 0
logs_json = []

for _, row in candidates.iterrows():
    to_addr = Web3.to_checksum_address(str(row["Address"]).strip())
    fn_name = str(row["Function"]).strip()

    # Minimal ABI for zero-arg function by name
    abi = [{
        "name": fn_name,
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [],
        "outputs": []
    }]
    contract = w3.eth.contract(address=to_addr, abi=abi)
    fn = contract.get_function_by_signature(f"{fn_name}()")()

    # Re-estimate on-chain
    try:
        est = w3.eth.estimate_gas({"from": from_addr, "to": to_addr, "data": fn._encode_transaction_data()})
    except Exception as e:
        print(f"❌ estimate_gas failed for {to_addr} {fn_name}(): {e.__class__.__name__}")
        continue

    gas_limit = int(min(GAS_CAP, math.ceil(est * GAS_BUMP)))
    base, prio, max_fee = calc_fees(w3)

    tx = {
        "from": from_addr,
        "to": to_addr,
        "nonce": nonce,
        "data": fn._encode_transaction_data(),
        "value": 0,
        "gas": gas_limit,
        "maxPriorityFeePerGas": prio,
        "maxFeePerGas": max_fee,
        "chainId": w3.eth.chain_id,
    }

    print(f"→ Prepared tx #{sent+1}: to={to_addr} fn={fn_name}() gas~{est} -> limit {gas_limit} "
          f"(base={base} prio={prio} maxFee={max_fee})")

    attempt = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "address": to_addr,
        "function": f"{fn_name}()",
        "gas_estimate": int(est),
        "gas_limit": gas_limit,
        "base_fee_gwei": round(gwei(base), 6),
        "priority_fee_gwei": round(gwei(prio), 6),
        "max_fee_gwei": round(gwei(max_fee), 6),
        "sent": 0,
        "tx_hash": "",
        "status": "DRY_RUN" if not ENABLE_SEND else "PENDING"
    }

    if not ENABLE_SEND:
        logs_json.append({**tx, "status": "DRY_RUN"})
        csv_log_row(attempt)
    else:
        try:
            signed = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            h = tx_hash.hex()
            print(f"   ✓ Sent: {h}")
            print(f"   🔗 Etherscan: https://etherscan.io/tx/{h}")
            nonce += 1
            status = "SENT"

            # Optional wait
            if os.getenv("WAIT_FOR_RECEIPT", "1") == "1":
                rcpt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
                status = f"MINED:{rcpt.status}"
                print(f"   ✓ Receipt status={rcpt.status} gasUsed={rcpt.gasUsed}")

            logs_json.append({**tx, "txHash": h, "status": status})
            attempt.update({"sent": 1, "tx_hash": h, "status": status})
            csv_log_row(attempt)
            sent += 1
        except Exception as e:
            err = f"SEND_FAIL:{e.__class__.__name__}"
            print(f"   ✗ Send failed: {e.__class__.__name__}: {e}")
            logs_json.append({**tx, "status": err})
            attempt.update({"sent": 1, "status": err})
            csv_log_row(attempt)

    if sent >= MAX_TX:
        break

# JSONL log (append)
Path(LOG_FILE_JSONL).parent.mkdir(parents=True, exist_ok=True)
with open(LOG_FILE_JSONL, "a") as f:
    for entry in logs_json:
        f.write(json.dumps(entry) + "\n")

print(f"🏁 Done. Sent={sent} | Log (jsonl) → {LOG_FILE_JSONL} | Log (csv) → {LOG_FILE_CSV}")
