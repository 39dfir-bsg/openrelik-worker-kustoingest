import os
import shutil
import subprocess
import time
from uuid import uuid4

from celery import signals
from celery.utils.log import get_task_logger

from openrelik_common.logging import Logger
from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files

import yaml
from pathvalidate import sanitize_filename

from .app import celery

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-kustoingest.tasks.kustoingest"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Kusto Ingestor",
    "description": "Ingests .csv files into a Kusto Cluster",
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

    # TODO: Update so that this reads the kusto URI
    # .openrelik-config 'hostname' key support
    prefix = ""
    config_item = next((f for f in input_files if f.get('display_name') == ".openrelik-config"), None)
    if config_item:
        try:
            with open(config_item.get('path'), "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)

            if isinstance(config_data, dict) and "hostname" in config_data:
                raw_hostname = str(config_data["hostname"]).strip()
                prefix = f"{sanitize_filename(raw_hostname)}_"
            else:
                logger.info("No 'hostname' key found in .openrelik-config file.")

        except yaml.YAMLError:
            logger.error(".openrelik-config is not a valid YAML file.")
        except Exception as e:
            logger.error(f"Error reading .openrelik-config: {e}")

    # Create temporary directory and hard link files for processing
    temp_dir = os.path.join(output_path, uuid4().hex)
    os.mkdir(temp_dir)
    # don't run on the .openrelik-config file
    # TODO: Update this logic
    for file in (f for f in input_files if f.get('display_name') != ".openrelik-config"):
        filename = os.path.basename(file.get("path"))
        os.link(file.get("path"), f"{temp_dir}/{filename}")
        # hard link files for processing

        # TODO: do the thing

    # Remove temp directory
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command="",
    )
