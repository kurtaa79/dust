#!/usr/bin/env python3
import os, re, json, math
import pandas as pd
from web3 import Web3

# Load environment variables
from dotenv import load_dotenv
load_dotenv()
MY_ADDRESS = Web3.to_checksum_address(os.getenv("MY_ADDRESS"))

# Config from env or defaults
REQUIRE_POSITIVE = bool(int(os.getenv("REQUIRE_POSITIVE", "1")))
MIN_AS_ETHER = float(os.getenv("MIN_AS_ETHER", "0"))
MAX_TX = int(os.getenv("MAX_TX", "5"))
PRIO_FEE_GWEI = float(os.getenv("PRIO_FEE_GWEI", "1.5"))
GAS_CAP = int(os.getenv("GAS_CAP", "60000"))
ENABLE_SEND = bool(int(os.getenv("ENABLE_SEND", "0")))

ALLOW_RE = re.compile(r"claim|getReward|withdraw|harvest|collect|release|redeem|payout|unstake|exit", re.I)
DENY_RE = re.compile(r"approve|set|owner|upgrade|pause|mint|burn|migrate|batch|initialize|permit|deposit|wrap|unwrap|transfer|tax|fee|fund|treasury|marketing|rewardpool|dividend|reflection|distribute", re.I)

rpc_url = os.getenv("RPC")
if not rpc_url:
    raise SystemExit("Missing RPC in env")

w3 = Web3(Web3.HTTPProvider(rpc_url))

# Load candidate CSV
candidates_file = "results/call_candidates.csv"
if not os.path.exists(candidates_file):
    raise SystemExit(f"No candidates file: {candidates_file}")

df = pd.read_csv(candidates_file)
total_before = len(df)

# Apply filters
filt = df["Function"].str.match(ALLOW_RE) & ~df["Function"].str.match(DENY_RE)
if REQUIRE_POSITIVE:
    filt &= (df["AsEther"].fillna(0) > MIN_AS_ETHER)

df = df[filt].copy()
print(f"[strict] Candidates after filters: {len(df)}/{total_before}")

# Sort by highest value first
df = df.sort_values(by="AsEther", ascending=False).head(MAX_TX)

# Prepare sending
def build_tx(row):
    fn = row["Function"].strip()
    addr = Web3.to_checksum_address(row["Address"])
    contract = w3.eth.contract(address=addr, abi=json.loads(row["ABI"]))
    # Only zero-arg or 1 address-arg
    sig = fn.split("(")[0]
    fndef = None
    for abifn in contract.abi:
        if abifn.get("type")=="function" and abifn.get("name")==sig:
            fndef = abifn
            break
    args = []
    if fndef and len(fndef.get("inputs", [])) == 1:
        if fndef["inputs"][0]["type"] == "address":
            args = [MY_ADDRESS]
        else:
            return None
    elif fndef and len(fndef.get("inputs", [])) > 0:
        return None
    try:
        gas_est = contract.get_function_by_signature(fn)(*args).estimate_gas({"from": MY_ADDRESS})
    except Exception:
        return None
    if gas_est > GAS_CAP:
        return None
    tx = contract.get_function_by_signature(fn)(*args).build_transaction({
        "from": MY_ADDRESS,
        "gas": gas_est,
        "maxPriorityFeePerGas": int(PRIO_FEE_GWEI * 1e9),
        "maxFeePerGas": int(w3.eth.gas_price + PRIO_FEE_GWEI * 1e9),
        "nonce": w3.eth.get_transaction_count(MY_ADDRESS)
    })
    return tx

# Send or dry-run
for _, row in df.iterrows():
    tx = build_tx(row)
    if not tx:
        continue
    if ENABLE_SEND:
        signed = w3.eth.account.sign_transaction(tx, private_key=os.getenv("PRIVATE_KEY"))
        tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
        print(f"✓ Sent: {tx_hash.hex()}")
    else:
        print(f"(dry-run) Would send: {row['Function']} to {row['Address']} value≈{row['AsEther']}")
