from azure.kusto.data.exceptions import KustoApiError
from azure.kusto.ingest import (
    KustoStreamingIngestClient,
    FileDescriptor,
)
import os
import re
import json
import time

# Map pandas dtype to Kusto type
dtype_map = {
    "int": "long",
    "float": "real",
    "bool": "bool",
    "datetime64": "datetime",
    "object": "string",
    "string": "string",
}

def _sanitize_column_name(name: str) -> str:
    # Replace '<' with 'lt'
    name = name.replace('<', 'lt')
    # Replace '>' with 'gt'
    name = name.replace('>', 'gt')
    # Replace all other characters that are not letters, digits, or underscore with '_'
    name = re.sub(r'[^A-Za-z0-9_]', '_', name)
    # Ensure it doesn't start with a digit (optional, Kusto requires columns to start with letter or _)
    if re.match(r'^[0-9]', name):
        name = '_' + name
    return name

def _pandas_to_kusto_dtype(dtype_str: str) -> str:
    for key, kusto_type in dtype_map.items():
        if key in dtype_str:
            return kusto_type
    return "string"  # fallback

def _wait_for_streaming_policy_ready(kusto_client, database, table_name, max_retries=5, retry_delay=5):
    """Wait until the table's streaming ingestion policy is enabled."""

    for attempt in range(1, max_retries + 1):
        try:
            results = kusto_client.execute_mgmt(
                database,
                f".show table {table_name} policy streamingingestion"
            )

            if results.primary_results[0].rows_count == 0:
                raise ValueError("No policy rows returned")

            row = results.primary_results[0].rows[0].to_dict()
            policy_raw = row.get("Policy")

            if not policy_raw:
                raise ValueError("Policy field is empty")

            # Safe JSON parse
            try:
                policy_json = json.loads(policy_raw) if isinstance(policy_raw, str) else policy_raw
            except json.JSONDecodeError:
                raise ValueError("Policy JSON invalid")

            if policy_json.get("IsEnabled"):
                print("✅ Table ready for streaming ingestion")
                return True
            else:
                raise ValueError("Streaming ingestion policy not enabled")

        except Exception as e:
            print(f"⚠️ Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
                retry_delay += retry_delay
            else:
                print("❌ Max retries reached, giving up.")
                return False

def _split_csv_file(
    input_csv: str,
    output_dir: str,
    chunk_size_bytes: int = 7500000,  # 7.5 MB
    ignore_header: bool = False
) -> list:
    """
    Split a CSV file into multiple smaller CSV files (~chunk_size_bytes each),
    optionally skipping the header in the first chunk.

    Returns:
        List of file paths for each chunk.
    """
    os.makedirs(output_dir, exist_ok=True)
    chunk_files = []
    csv_name = os.path.basename(input_csv)

    with open(input_csv, "r", encoding="utf-8") as f:
        header = f.readline() if ignore_header else None
        chunk_index = 1
        current_chunk_size = 0
        lines_buffer = []

        for line in f:
            lines_buffer.append(line)
            current_chunk_size += len(line.encode("utf-8"))

            if current_chunk_size >= chunk_size_bytes:
                chunk_file = os.path.join(output_dir, f"chunk_{chunk_index}_{csv_name}")
                with open(chunk_file, "w", encoding="utf-8") as cf:
                    cf.writelines(lines_buffer)
                chunk_files.append(chunk_file)
                print(f"✅ Written {chunk_file} ({current_chunk_size / 1024:.2f} KB)")

                # Reset buffer
                chunk_index += 1
                current_chunk_size = 0
                lines_buffer = []

        # Write any remaining lines
        if lines_buffer:
            chunk_file = os.path.join(output_dir, f"chunk_{chunk_index}_{csv_name}")
            with open(chunk_file, "w", encoding="utf-8") as cf:
                cf.writelines(lines_buffer)
            chunk_files.append(chunk_file)
            print(f"✅ Written {chunk_file} ({current_chunk_size / 1024:.2f} KB)")

    return chunk_files

def _ingest_data_via_streaming(kusto_ingest_client, database, table_name, ingestion_props, csv_path, max_retries=5, retry_delay=5):
    """Ingest data via streaming with error fallback"""

    for attempt in range(1, max_retries + 1):
        try:
            file_descriptor = FileDescriptor(csv_path, os.path.getsize(csv_path))
            results = kusto_ingest_client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)
            print("✅ Ingesting data")
            return True
        except Exception as e:
            print(f"⚠️ Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
                retry_delay += retry_delay
            else:
                print("❌ Max retries reached, giving up.")
                return False