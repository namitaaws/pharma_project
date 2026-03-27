import json
import pandas as pd


def convert_json_string_to_parquet(json_string, parquet_file_path):
    """Convert JSON string to Parquet file."""
    df = pd.read_json(json_string)
    df.to_parquet(parquet_file_path, index=False)
    print(f"Converted in-memory JSON to Parquet: {parquet_file_path}")


def convert_json_file_to_parquet(json_file_path, parquet_file_path):
    """Convert local JSON file to Parquet file."""
    df = pd.read_json(json_file_path)
    df.to_parquet(parquet_file_path, index=False)
    print(f"Converted {json_file_path} -> {parquet_file_path}")


if __name__ == "__main__":
    # Adjust paths as needed
    source_json_path = "silver_layer_file/customers.json"  # Local copy of silver file
    target_parquet_path = "silver_layer_file/customers.parquet"

    convert_json_file_to_parquet(source_json_path, target_parquet_path)
