TARGETED PIPELINE README

Requirements:
    pip install pandas web3 requests eth-utils

Environment variables:
    export ETH_RPC_URL="https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
    export ETHERSCAN_API_KEY="YOUR_KEY"

Usage:
    python run_targeted_pipeline.py --skip-scan --limit 500 --sleep 0.6

Steps:
    1. (optional) Run sniper to produce results/dust_sniper_results_v2.csv
    2. extract_high_value_contracts.py
    3. dust_enricher_targeted.py
    4. abi_signature_extractor_targeted.py
    5. selector_match_scanner_targeted.py


source venv/bin/activate


_8fncQfovghF7qlIywboc

9MWW94S63UBRSUX4MR56A7XKM58WPWE9WJ