import os
import pandas as pd
import json

def csv_to_json(csv_path, json_path):
    df = pd.read_csv(csv_path)
    records = df.to_dict(orient='records')
    
    with open(json_path, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')

    print(f"✅ Converted CSV to JSONL: {json_path}")

def convert_all_samples(base_dir):
    for folder in os.listdir(base_dir):
        sample_path = os.path.join(base_dir, folder)
        if os.path.isdir(sample_path) and folder.startswith("sample_"):
            offset = folder.split("_")[-1]
            csv_file = os.path.join(sample_path, f"breathe_dataset_offset_{offset}.csv")
            json_file = os.path.join(sample_path, f"breathe_dataset_offset_{offset}.json")
            
            if os.path.exists(csv_file):
                if not os.path.exists(json_file):  # prevent overwriting
                    csv_to_json(csv_file, json_file)
                else:
                    print(f"JSON already exists: {json_file}")
            else:
                print(f"CSV not found: {csv_file}")

if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(__file__))  # goes up from ingest/
    data_dir = os.path.join(project_root, "data")
    convert_all_samples(data_dir)
