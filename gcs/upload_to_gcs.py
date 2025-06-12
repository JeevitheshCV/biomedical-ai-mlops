import os
from google.cloud import storage
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), '..', os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "bio-med")

def upload_directory_to_gcs(local_dir, gcs_path_prefix):
    # Auth
    client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
    bucket = client.get_bucket(BUCKET_NAME)

    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            gcs_blob_path = os.path.join(gcs_path_prefix, relative_path)

            blob = bucket.blob(gcs_blob_path)
            blob.upload_from_filename(local_path)

            print(f"📤 Uploaded: {local_path} → gs://{BUCKET_NAME}/{gcs_blob_path}")

if __name__ == "__main__":
    base_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    upload_directory_to_gcs(base_dir, 'data')
