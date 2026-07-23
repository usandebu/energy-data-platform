from pathlib import Path

import pytest

from energy_pipeline.ingest.config import (
    parse_raw_bucket,
    parse_raw_root,
    parse_raw_storage_backend,
)


def test_parse_raw_root_returns_path():
    assert parse_raw_root("data/raw") == Path("data/raw")


def test_parse_raw_root_rejects_empty_value():
    with pytest.raises(ValueError, match="RAW_ROOT cannot be empty"):
        parse_raw_root("  ")


def test_parse_raw_storage_backend_accepts_supported_values():
    assert parse_raw_storage_backend("local") == "local"
    assert parse_raw_storage_backend(" S3 ") == "s3"


def test_parse_raw_storage_backend_rejects_unsupported_value():
    with pytest.raises(ValueError, match="RAW_STORAGE_BACKEND must be local or s3"):
        parse_raw_storage_backend("filesystem")


def test_parse_raw_bucket_ignores_bucket_for_local_backend():
    assert parse_raw_bucket("unused", "local") is None


def test_parse_raw_bucket_requires_bucket_for_s3_backend():
    with pytest.raises(
        ValueError,
        match="RAW_BUCKET is required when RAW_STORAGE_BACKEND=s3",
    ):
        parse_raw_bucket(" ", "s3")


def test_parse_raw_bucket_returns_bucket_for_s3_backend():
    assert parse_raw_bucket(" test-raw-bucket ", "s3") == (
        "test-raw-bucket"
    )
