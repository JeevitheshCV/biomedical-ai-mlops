import os
import json
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from dotenv import load_dotenv
import ftfy
import re
import unicodedata

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BATCH_SIZE = 1000
PROGRESS_PATH = os.path.join("data", "progress.json")

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_progress():
    if os.path.exists(PROGRESS_PATH):
        with open(PROGRESS_PATH, 'r') as f:
            return json.load(f)
    else:
        return {"offset": 0}

def save_progress(progress):
    with open(PROGRESS_PATH, 'w') as f:
        json.dump(progress, f)

def clean_text(text):
    if pd.isnull(text):
        return text
    text = ftfy.fix_text(str(text))
    text = re.sub(r"[^a-zA-Z0-9\s,.!?:;'\"()\-–—%]", "", text)
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')

def main():
    progress = load_progress()
    offset = progress["offset"]

    print(f"🔄 Fetching batch from OFFSET {offset}")

    query = f"""
    SELECT
      id,
      title,
      abstract,
      authors,
      keywords,
      organization_affiliated
    FROM
      `bigquery-public-data.breathe.nature`
    WHERE
      abstract IS NOT NULL
      AND authors IS NOT NULL
      AND keywords IS NOT NULL
      AND organization_affiliated IS NOT NULL
      AND title IS NOT NULL
      AND id IS NOT NULL
    ORDER BY id
    LIMIT {BATCH_SIZE}
    OFFSET {offset}
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        print("✅ All records processed. No more data.")
        return

    # Clean the text
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(clean_text)

    # Save cleaned batch
    csv_path = f"data/breathe_batch_{offset}.csv"
    df.to_csv(csv_path, index=False)
    print(f"✅ Saved batch to {csv_path}")

    # Update progress
    progress["offset"] += BATCH_SIZE
    save_progress(progress)
    print(f"📌 Progress updated. Next offset: {progress['offset']}")

if __name__ == "__main__":
    main()
