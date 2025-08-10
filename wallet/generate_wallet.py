from eth_account import Account
import secrets
import os

# Define wallet path
wallet_dir = os.path.join(os.path.dirname(__file__))
env_path = os.path.join(wallet_dir, ".env")

# Generate a secure private key
private_key = "0x" + secrets.token_hex(32)
account = Account.from_key(private_key)
address = account.address

# Save to .env in wallet directory
with open(env_path, "w") as f:
    f.write(f"PRIVATE_KEY={private_key}\n")
    f.write(f"ADDRESS={address}\n")

print("✅ Wallet generated successfully!")
print("Your public address is:", address)
print("Your private key has been saved to wallet/.env (do not share it!)")
