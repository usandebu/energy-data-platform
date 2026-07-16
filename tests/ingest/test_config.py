from pathlib import Path

import pytest

from energy_pipeline.ingest.config import parse_raw_root


def test_parse_raw_root_returns_path():
    assert parse_raw_root("data/raw") == Path("data/raw")


def test_parse_raw_root_rejects_empty_value():
    with pytest.raises(ValueError, match="RAW_ROOT cannot be empty"):
        parse_raw_root("  ")
