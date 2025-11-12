import os
import shutil
import subprocess
import time
import re
from uuid import uuid4

from celery import signals
from celery.utils.log import get_task_logger

from openrelik_common.logging import Logger
from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files

import yaml

from pathvalidate import sanitize_filename
from .utils import _sanitize_column_name, _pandas_to_kusto_dtype, _wait_for_streaming_policy_ready, _split_csv_file, _ingest_data_via_streaming

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, DataFormat
from azure.kusto.data.exceptions import KustoApiError
from azure.kusto.ingest import (
    KustoStreamingIngestClient,
    IngestionProperties,
    FileDescriptor,
)
import pandas as pd
import json

from .app import celery

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-kustoingest.tasks.kustoingest"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Kusto Ingestor",
    "description": "Ingests .csv files into a Kusto Cluster",
    "task_config": [
        {
            "name": "connection_override",
            "label": "Connection URI Override",
            "description": "Overide the default connection URI (http://localhost:8080) and any .openrelik-config URIs",
            "type": "text",
            "required": False,
        },
        {
            "name": "database_override",
            "label": "Database Override",
            "description": "Overide the default database (Default) and any .openrelik-config set database",
            "type": "text",
            "required": False,
        },
    ]
}

COMPATIBLE_INPUTS = {
    "data_types": [],
    "mime_types": ["application/octet-stream", "text/plain"],
    "filenames": [
        "*.csv",
        ".openrelik-config",
        ],
}

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))

@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )

@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def kustoingest(
    self,
    pipe_result=None,
    input_files=[],
    output_path=None,
    workflow_id=None,
    task_config={},
) -> str:
    output_files = []
    input_files = get_input_files(pipe_result, input_files or [], filter=COMPATIBLE_INPUTS)
    if not input_files:
        return create_task_result(
            output_files=output_files,
            workflow_id=workflow_id,
            command="",
        )

    # TODO: Update so that this reads the kusto URI + database
    # .openrelik-config 'openrelik-kusto-cluster-uri' and 'openrelik-kusto-database' key support
    database = "Default"
    cluster = "http://localhost:8080"
    config_item = next((f for f in input_files if f.get('display_name') == ".openrelik-config"), None)
    if config_item:
        try:
            with open(config_item.get('path'), "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)

            if isinstance(config_data, dict) and "openrelik-kusto-cluster-uri" in config_data:
                cluster = config_data["openrelik-kusto-cluster-uri"]
            else:
                logger.info("No 'cluster' key found in .openrelik-config file, falling back to default/provided")

            if isinstance(config_data, dict) and "openrelik-kusto-database" in config_data:
                database = config_data["openrelik-kusto-database"]
            else:
                logger.info("No 'database' key found in .openrelik-config file, falling back to default/provided")

        except yaml.YAMLError:
            logger.error(".openrelik-config is not a valid YAML file.")
        except Exception as e:
            logger.error(f"Error reading .openrelik-config: {e}")
    
    # Override database and uri if provided
    if task_config.get("database_override", None):
        database = task_config.get("database_override", None)
    if task_config.get("connection_override", None):
        cluster = task_config.get("connection_override", None)

    # No auth (for internal or test setups)
    kcsb = KustoConnectionStringBuilder.with_no_authentication(cluster)

    # Create temporary directory and hard link files for processing
    temp_dir = os.path.join(output_path, uuid4().hex)
    os.mkdir(temp_dir)
    # don't run on the .openrelik-config file
    # TODO: Update this logic
    for file in (f for f in input_files if f.get('display_name') != ".openrelik-config"):
        filename = os.path.basename(file.get("path"))
        os.link(file.get("path"), f"{temp_dir}/{filename}")
        # hard link files for processing

        # TODO: make this better
        # Get table name from csv file name
        table_name = os.path.splitext(file.get('display_name'))[0]

        # Step 1: Replace dollar signs with 'ds'
        table_safe_name = re.sub(r'\$', 'ds', table_name)
        # Step 2: Replace any other invalid characters with underscore
        table_safe_name = re.sub(r'[^A-Za-z0-9_]', '_', table_safe_name)

        csv_path = file.get("path")
        chunk_output_dir = os.path.join(temp_dir, uuid4().hex)

        # infer table schema from first 1k rows
        df = pd.read_csv(csv_path, nrows=1000, parse_dates=True)
        # Build Kusto table schema definition
        table_definition = ", ".join(
            f"[{_sanitize_column_name(col)}]:{_pandas_to_kusto_dtype(str(dtype))}" for col, dtype in df.dtypes.items()
        )

        # create-merge in case exists
        create_table_command = f".create-merge table {table_safe_name} ({table_definition})"

        # create ingest table with single dynamic column
        with KustoClient(kcsb) as kusto_client:
            try:
                kusto_client.execute_mgmt(database, create_table_command)
                print("✅ Table create command issued")
                print(f"   Create table command: {create_table_command}")
            except Exception as e:
                # TODO: check error type and handle properly
                # Assume error is because database isn't attached after reboot
                print(f"⚠️ Create command failed: {e}")
                print(f"⚠️ Assuming database not attached, will attach and try again")
                # assume filepath and naming scheme
                kusto_client.execute_mgmt("NetDefaultDB", f".attach database {database} from @\"/data/dbs/{database}/md\" ")
                print("✅ Reattach database command issued")
                # TODO: poll this with timeout
                # sleep 5 to let metadata refresh
                time.sleep(5)
                kusto_client.execute_mgmt(database, create_table_command)
                print("✅ Table create command issued")

            # enable streaming for table
            kusto_client.execute_mgmt(database, f"""
            .alter-merge table {table_safe_name} policy streamingingestion @\'{{\"IsEnabled\":true}}\'
            """)
            print(f"✅ Enabling streamingingestion policy")

            # force table index from metadata
            results = kusto_client.execute_mgmt(database, f"""
            .show table {table_safe_name} schema as json
            """)
            print(f"✅ Force indexing table")

            # check and see if streaming enabled
            # TODO: this returns true if it worked, false if timed out, so set up exit or fail or something
            _wait_for_streaming_policy_ready(kusto_client, database, table_safe_name)

        # TODO: Streaming does not support ignore first record so we will have to manually strip this
        ingestion_props = IngestionProperties(
                database=database,
                table=table_safe_name,
                data_format=DataFormat.CSV,
            )

        with KustoStreamingIngestClient(kcsb) as kusto_ingest_client:
            # Split file into 7.5KB chunks and also ignore header for first file
            ingore_header = True

            # Create chunks folder if it doesn't exist
            os.makedirs(chunk_output_dir, exist_ok=True)
            # when chunking, ignore first line (header) if required
            chunk_files = _split_csv_file(csv_path, chunk_output_dir, ignore_header=True)

            for csv_chunk_path in chunk_files:
            # TODO: this returns true if it worked, false if timed out, so set up exit or fail or something
                _ingest_data_via_streaming(kusto_ingest_client, database, table_safe_name, ingestion_props, csv_chunk_path, max_retries=10)

            # remove chunks directory
            if os.path.exists(chunk_output_dir):
                shutil.rmtree(chunk_output_dir)

    # Remove temp directory
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command="",
    )
