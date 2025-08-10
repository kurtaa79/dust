
#!/usr/bin/env python3
import asyncio, aiohttp, csv, os, sys, time, math
from pathlib import Path
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()

# Paths
BASE = Path(__file__).resolve().parent
STATE_FILE = (BASE / ".." / "state" / "last_block.txt").resolve()
RESULTS_DIR = (BASE / ".." / "results").resolve()
OUT_FILE = RESULTS_DIR / "dust_sniper_results_v2.csv"

# Config
RPC_URL = os.getenv("WEB3_PROVIDER") or os.getenv("ETH_RPC_URL")
if not RPC_URL:
    sys.exit("❌ Set WEB3_PROVIDER or ETH_RPC_URL in your .env")

DUST_MIN = float(os.getenv("DUST_MIN", "1e-5"))
DUST_MAX = float(os.getenv("DUST_MAX", "0.01"))
WORKERS   = int(os.getenv("WORKERS", "8"))
FOLLOW    = int(os.getenv("FOLLOW", "0"))  # 1 = keep tailing new blocks
BATCH     = max(WORKERS, 8)                # batch size equals workers
LOG_EVERY = int(os.getenv("LOG_EVERY", "10"))  # batches between progress logs

w3 = Web3(Web3.HTTPProvider(RPC_URL))

def read_last_block(default_lag=1000):
    try:
        if STATE_FILE.exists():
            n = int(STATE_FILE.read_text().strip())
            return max(0, n)
    except Exception:
        pass
    head = w3.eth.block_number
    return max(0, head - default_lag)

def write_last_block(n: int):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(str(n), encoding="utf-8")

async def fetch_block(session, bn: int):
    payload = {"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":[hex(bn), True],"id":bn}
    async with session.post(RPC_URL, json=payload, timeout=90) as resp:
        data = await resp.json()
        return data.get("result")

def ensure_output():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    write_header = not OUT_FILE.exists()
    f = open(OUT_FILE, "a", newline="")
    writer = csv.writer(f)
    if write_header:
        writer.writerow(["block", "tx_hash", "value_eth", "from", "to"])
        f.flush()
    return f, writer

async def process_block(session, bn: int, writer, seen: set):
    blk = await fetch_block(session, bn)
    if not blk or "transactions" not in blk:
        return 0
    found = 0
    for tx in blk["transactions"]:
        to_ = tx.get("to")
        val = tx.get("value")
        if not to_ or not val:
            continue
        try:
            eth_val = float(Web3.from_wei(int(val, 16), "ether"))
        except Exception:
            continue
        if DUST_MIN <= eth_val <= DUST_MAX:
            txh = tx.get("hash")
            if txh not in seen:
                seen.add(txh)
                writer.writerow([bn, txh, eth_val, tx.get("from"), to_])
                found += 1
    return found

def fmt_eta(seconds: float) -> str:
    if seconds < 0: seconds = 0
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

async def scan_range(start_bn: int, end_bn: int):
    total_blocks = max(0, end_bn - start_bn + 1)
    batches_total = math.ceil(total_blocks / BATCH) if total_blocks else 0
    batch_index = 0

    f, writer = ensure_output()
    seen = set()
    total_found = 0
    t0 = time.time()

    async with aiohttp.ClientSession() as session:
        for batch_start in range(start_bn, end_bn + 1, BATCH):
            batch_end = min(batch_start + BATCH - 1, end_bn)
            t_batch = time.time()
            tasks = [process_block(session, bn, writer, seen) for bn in range(batch_start, batch_end + 1)]
            results = await asyncio.gather(*tasks)
            batch_found = sum(results)
            total_found += batch_found

            # flush + save state
            f.flush()
            write_last_block(batch_end)

            # progress metrics
            batch_index += 1
            done_blocks = min(total_blocks, batch_index * BATCH)
            progress = (done_blocks / total_blocks) * 100 if total_blocks else 100.0
            elapsed = time.time() - t0
            avg_per_batch = elapsed / batch_index
            eta = avg_per_batch * (batches_total - batch_index) if batches_total else 0

            # log every LOG_EVERY batches (and first/last)
            if batch_index == 1 or batch_index % LOG_EVERY == 0 or done_blocks >= total_blocks:
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"Blocks {batch_start}-{batch_end} | found {batch_found} | "
                    f"progress {progress:.1f}% ({done_blocks}/{total_blocks}) | "
                    f"elapsed {fmt_eta(elapsed)} | ETA {fmt_eta(eta)} | "
                    f"saved last_block={batch_end}",
                    flush=True
                )

    f.close()
    return total_found

async def follow_tip(start_bn: int):
    current = start_bn
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 📡 Follow mode ON — tailing new blocks...")
    while True:
        try:
            head = w3.eth.block_number
            if head >= current:
                await scan_range(current, head)
                current = head + 1
        except Exception as e:
            print(f"⚠️ follow_tip error: {e}")
        await asyncio.sleep(5)

async def main():
    head = w3.eth.block_number
    start_bn = read_last_block()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Dust sniper (stateful, verbose)")
    print(f"  RPC={RPC_URL}")
    print(f"  Start {start_bn} → Head {head} (about {max(0, head-start_bn)} blocks) | batch={BATCH}, workers={WORKERS}")
    print(f"  Dust band: {DUST_MIN}–{DUST_MAX} ETH")
    print(f"  Output: {OUT_FILE}")
    print(f"  State file: {STATE_FILE}")
    if FOLLOW:
        await follow_tip(start_bn)
    else:
        total = await scan_range(start_bn, head)
        print(f"✅ Finished. Found {total} dust txs. Updated {STATE_FILE} and appended to {OUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
