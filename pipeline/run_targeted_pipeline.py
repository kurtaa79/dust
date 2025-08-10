import subprocess

subprocess.run(["python", "extract_high_value_contracts.py"], check=True)
subprocess.run(["python", "dust_enricher_targeted.py"], check=True)
subprocess.run(["python", "abi_signature_extractor_targeted.py"], check=True)
subprocess.run(["python", "selector_match_scanner_targeted.py"], check=True)

