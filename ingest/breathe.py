import os
import json
import pandas as pd
import ftfy
import re
import unicodedata
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), '..', os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
DATASET = os.getenv("BQ_DATASET", "breathe")
TABLE = os.getenv("BQ_TABLE", "nature")
LIMIT = int(os.getenv("BATCH_SIZE", 1000))

# Track processed offset
PROGRESS_PATH = "progress.json"
def get_offset():
    if os.path.exists(PROGRESS_PATH):
        with open(PROGRESS_PATH, "r") as f:
            return json.load(f).get("offset", 0)
    return 0

def update_offset(offset):
    with open(PROGRESS_PATH, "w") as f:
        json.dump({"offset": offset}, f)

# Text cleanup functions
def fix_encoding(text):
    return ftfy.fix_text(str(text)) if pd.notnull(text) else text

def clean_special_chars(text):
    return re.sub(r"[^a-zA-Z0-9\s,.!?:;'\"()\\-–—%]", "", text) if pd.notnull(text) else text

def remove_unicode(text):
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii') if pd.notnull(text) else text

def clean_dataframe(df):
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].apply(fix_encoding).apply(clean_special_chars).apply(remove_unicode)
    return df

def main():
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    offset = get_offset()
    print(f"🔄 Fetching batch from OFFSET {offset}")

    query = f"""
        SELECT id, title, abstract, authors, keywords, organization_affiliated
        FROM `bigquery-public-data.{DATASET}.{TABLE}`
        WHERE abstract IS NOT NULL
        AND authors IS NOT NULL
        AND keywords IS NOT NULL
        AND organization_affiliated IS NOT NULL
        AND title IS NOT NULL
        AND id IS NOT NULL
        LIMIT {LIMIT} OFFSET {offset}
    """

    df = client.query(query).to_dataframe()
    if df.empty:
        print("✅ No more records to process. You're done!")
        return

    print(f"✅ Retrieved {len(df)} records")
    df = clean_dataframe(df)

    # Create output folder: data/sample_<offset>/
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', f'sample_{offset}'))
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, f"breathe_dataset_offset_{offset}.csv")
    df.to_csv(output_file, index=False)
    print(f"Saved cleaned data to {output_file}")

    update_offset(offset + LIMIT)

if __name__ == "__main__":
    main()
