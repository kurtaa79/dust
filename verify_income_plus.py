import os
import sys
import csv
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import requests
import pandas as pd
from dotenv import load_dotenv
from web3 import Web3

SEND_LOG = "results/send_attempts_log.csv"
OUT_LOG = "results/income_log.csv"

TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()

def ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def read_latest_txhash(path: str) -> Optional[str]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p)
        if "tx_hash" in df.columns and not df["tx_hash"].isna().all():
            df = df.dropna(subset=["tx_hash"])
            if "timestamp" in df.columns:
                df = df.sort_values("timestamp", ascending=False)
            return str(df.iloc[0]["tx_hash"]).strip()
    except Exception:
        return None
    return None

def etherscan_internal_by_txhash(api_key: str, txhash: str) -> Dict[str, Any]:
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "txlistinternal",
        "txhash": txhash,
        "apikey": api_key,
    }
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1.5)
    return {"status":"0","message":"error","result":[]}

def wei_to_eth(wei: str) -> float:
    try: return int(wei) / 1e18
    except Exception: return 0.0

def decode_topic_address(topic_hex: str) -> str:
    # last 20 bytes of the 32-byte topic
    return Web3.to_checksum_address("0x" + topic_hex[-40:])

def get_token_meta(w3: Web3, token_addr: str) -> Dict[str, Any]:
    # Try ERC-20 symbol() and decimals(). Fail-safe defaults.
    abi = [
        {"name":"symbol","outputs":[{"type":"string"}],"inputs":[],"stateMutability":"view","type":"function"},
        {"name":"decimals","outputs":[{"type":"uint8"}],"inputs":[],"stateMutability":"view","type":"function"},
        {"name":"name","outputs":[{"type":"string"}],"inputs":[],"stateMutability":"view","type":"function"},
    ]
    ct = w3.eth.contract(address=Web3.to_checksum_address(token_addr), abi=abi)
    meta = {"symbol":"", "decimals":18, "name":""}
    for f, key in [(ct.functions.symbol, "symbol"), (ct.functions.decimals, "decimals"), (ct.functions.name, "name")]:
        try:
            v = f().call()
            meta[key] = v
        except Exception:
            pass
    # sanitize
    try: meta["decimals"] = int(meta.get("decimals", 18))
    except Exception: meta["decimals"] = 18
    return meta

def human_amount(value_raw: int, decimals: int) -> float:
    try:
        return float(value_raw) / (10 ** decimals)
    except Exception:
        return float(value_raw)

def verify_income(txhash: str, to_addr: str, api_key: Optional[str], rpc_url: str) -> Dict[str, Any]:
    out = {
        "tx_hash": txhash,
        "eth_received": 0.0,
        "token_transfers_in": [],  # list of dicts
        "notes": []
    }
    to_addr_lc = to_addr.lower()

    # 1) ETH internal transfers via Etherscan (if API key is present)
    if api_key:
        try:
            data = etherscan_internal_by_txhash(api_key, txhash)
            if data.get("status") == "1":
                for itx in data.get("result", []):
                    if str(itx.get("to","")).lower() == to_addr_lc:
                        out["eth_received"] += wei_to_eth(itx.get("value","0"))
            else:
                out["notes"].append(f"etherscan_internal_status={data.get('status')} msg={data.get('message')}")
        except Exception as e:
            out["notes"].append(f"etherscan_error:{e.__class__.__name__}")

    # 2) ERC-20 transfers via receipt logs (RPC)
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
    rcpt = w3.eth.get_transaction_receipt(txhash)
    # Iterate logs for Transfer events
    for lg in rcpt.logs:
        if not lg["topics"]: 
            continue
        if lg["topics"][0].hex() != TRANSFER_TOPIC:
            continue
        # topics: [event, from, to]; data = value
        try:
            to_topic = lg["topics"][2].hex()
            to_decoded = decode_topic_address(to_topic)
            if to_decoded.lower() != to_addr_lc:
                continue
        except Exception:
            continue
        token_addr = lg["address"]
        # value is in data (hex)
        try:
            raw_val = int(lg["data"], 16)
        except Exception:
            raw_val = 0
        meta = get_token_meta(w3, token_addr)
        amt = human_amount(raw_val, meta["decimals"])
        out["token_transfers_in"].append({
            "token": token_addr,
            "symbol": meta.get("symbol",""),
            "name": meta.get("name",""),
            "raw_value": raw_val,
            "decimals": meta.get("decimals", 18),
            "amount": amt
        })

    return out

def main():
    load_dotenv()
    api_key = os.getenv("ETHERSCAN_API_KEY")  # optional
    rpc = os.getenv("WEB3_PROVIDER_URL") or os.getenv("WEB3_PROVIDER") or os.getenv("ETH_RPC_URL")
    if not rpc:
        sys.exit("❌ RPC missing: set WEB3_PROVIDER_URL (or WEB3_PROVIDER / ETH_RPC_URL)")
    to_addr = os.getenv("FROM_ADDRESS")
    if not to_addr:
        sys.exit("❌ FROM_ADDRESS missing in .env")

    # txhash from CLI or last sent in attempts log
    txhash = sys.argv[1].strip() if len(sys.argv) > 1 else read_latest_txhash(SEND_LOG)
    if not txhash:
        sys.exit("❌ No tx hash provided and none found in send_attempts_log.csv")

    print(f"🔎 Verifying income for tx: {txhash}")
    res = verify_income(txhash, to_addr, api_key, rpc)

    # Console summary
    if res["eth_received"] > 0:
        print(f"✅ ETH income: {res['eth_received']:.6f} ETH")
    else:
        print("➖ No ETH internal transfers detected.")

    if res["token_transfers_in"]:
        print(f"✅ Token transfers to you: {len(res['token_transfers_in'])}")
        for t in res["token_transfers_in"]:
            sym = t.get("symbol") or ""
            print(f"   • {t['amount']:.6f} {sym} (token {t['token']})")
    else:
        print("➖ No ERC-20 Transfer events to your address in this tx.")

    # Append to CSV log
    path = Path(OUT_LOG)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp","tx_hash","address","eth_received","token_count","token_details_json","notes"])
        if write_header:
            w.writeheader()
        w.writerow({
            "timestamp": ts(),
            "tx_hash": res["tx_hash"],
            "address": to_addr,
            "eth_received": f"{res['eth_received']:.18f}",
            "token_count": len(res["token_transfers_in"]),
            "token_details_json": json.dumps(res["token_transfers_in"]),
            "notes": ";".join(res.get("notes", [])),
        })

    print(f"🧾 Logged to: {OUT_LOG}")

if __name__ == "__main__":
    main()
