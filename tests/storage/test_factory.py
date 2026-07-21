import pytest

from energy_pipeline.storage.factory import build_raw_storage
from energy_pipeline.storage.local import LocalRawStorage
from energy_pipeline.storage.s3 import S3RawStorage


def test_build_raw_storage_returns_local_storage(tmp_path):
    storage = build_raw_storage(
        backend="local",
        raw_root=tmp_path,
    )

    assert isinstance(storage, LocalRawStorage)
    assert storage.raw_root == tmp_path


def test_build_raw_storage_returns_s3_storage(tmp_path):
    storage = build_raw_storage(
        backend="s3",
        raw_root=tmp_path,
        raw_bucket="energy-data-platform-dev-raw",
    )

    assert isinstance(storage, S3RawStorage)
    assert storage.bucket == "energy-data-platform-dev-raw"


def test_build_raw_storage_requires_bucket_for_s3(tmp_path):
    with pytest.raises(
        ValueError,
        match="RAW_BUCKET is required when RAW_STORAGE_BACKEND=s3",
    ):
        build_raw_storage(
            backend="s3",
            raw_root=tmp_path,
        )


def test_build_raw_storage_rejects_unsupported_backend(tmp_path):
    with pytest.raises(ValueError, match="RAW_STORAGE_BACKEND must be local or s3"):
        build_raw_storage(
            backend="filesystem",
            raw_root=tmp_path,
        )
