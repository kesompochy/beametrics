from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

import yaml
from google.cloud import storage


def load_yaml_config(path: str) -> Dict[Any, Any]:
    """Load YAML configuration from GCS or local filesystem.

    Args:
        path: Path to YAML config file (gs://bucket/path/to/config.yaml or /path/to/config.yaml)

    Returns:
        Dict containing the YAML configuration
    """
    parsed = urlparse(path)

    if parsed.scheme == "gs":
        return _load_from_gcs(path)
    else:
        return _load_from_local(path)


def _load_from_gcs(gcs_path: str) -> Dict[Any, Any]:
    client = storage.Client()
    bucket_name = gcs_path.split("/")[2]
    blob_path = "/".join(gcs_path.split("/")[3:])

    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_path)

    return yaml.safe_load(blob.download_as_string())


def _load_from_local(file_path: str) -> Dict[Any, Any]:
    with Path(file_path).open("r") as f:
        return yaml.safe_load(f)
