import os
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

# Load environment
load_dotenv()
creds_path = os.path.join(os.path.dirname(__file__), '..', os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
print(f"🔐 Using credentials file: {creds_path}")

# Auth test
credentials = service_account.Credentials.from_service_account_file(creds_path)
client = storage.Client(credentials=credentials)
buckets = list(client.list_buckets())
print("✅ Successfully listed buckets!")
for b in buckets:
    print("-", b.name)
