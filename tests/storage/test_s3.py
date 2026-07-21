import json
from datetime import date
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from energy_pipeline.storage.raw import RawObjectKey
from energy_pipeline.storage.s3 import S3RawStorage


def test_s3_raw_storage_checks_data_object_existence():
    client = Mock()
    key = RawObjectKey(
        source="ree",
        dataset="balance-electrico",
        date=date(2024, 1, 2),
    )
    storage = S3RawStorage(bucket="energy-data-platform-dev-raw", client=client)

    assert storage.exists(key) is True

    client.head_object.assert_called_once_with(
        Bucket="energy-data-platform-dev-raw",
        Key="ree/balance-electrico/year=2024/month=01/day=02/data.json",
    )


def test_s3_raw_storage_returns_false_when_data_object_does_not_exist():
    client = Mock()
    client.head_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "404",
                "Message": "Not Found",
            }
        },
        "HeadObject",
    )
    key = RawObjectKey(
        source="aemet",
        dataset="climatologia-diaria",
        date=date(2024, 1, 2),
    )
    storage = S3RawStorage(bucket="energy-data-platform-dev-raw", client=client)

    assert storage.exists(key) is False


def test_s3_raw_storage_reraises_unexpected_client_errors():
    client = Mock()
    client.head_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access Denied",
            }
        },
        "HeadObject",
    )
    key = RawObjectKey(
        source="ree",
        dataset="balance-electrico",
        date=date(2024, 1, 2),
    )
    storage = S3RawStorage(bucket="energy-data-platform-dev-raw", client=client)

    with pytest.raises(ClientError):
        storage.exists(key)


def test_s3_raw_storage_saves_data_and_metadata_objects():
    client = Mock()
    payload = {
        "data": {"type": "Balance de energía eléctrica"},
        "included": [],
    }
    key = RawObjectKey(
        source="ree",
        dataset="balance-electrico",
        date=date(2024, 1, 2),
    )
    storage = S3RawStorage(bucket="energy-data-platform-dev-raw", client=client)

    destination = storage.save_object(payload, key)

    assert destination == (
        "s3://energy-data-platform-dev-raw/"
        "ree/balance-electrico/year=2024/month=01/day=02/data.json"
    )
    assert storage.data_uri(key) == destination
    data_call, metadata_call = client.put_object.call_args_list

    assert data_call.kwargs == {
        "Bucket": "energy-data-platform-dev-raw",
        "Key": "ree/balance-electrico/year=2024/month=01/day=02/data.json",
        "Body": json.dumps(payload, ensure_ascii=False, indent=2),
        "ContentType": "application/json",
    }

    assert metadata_call.kwargs["Bucket"] == "energy-data-platform-dev-raw"
    assert metadata_call.kwargs["Key"] == (
        "ree/balance-electrico/year=2024/month=01/day=02/metadata.json"
    )
    assert metadata_call.kwargs["ContentType"] == "application/json"

    metadata = json.loads(metadata_call.kwargs["Body"])
    assert metadata["source"] == "ree"
    assert metadata["dataset"] == "balance-electrico"
    assert metadata["requested_date"] == "2024-01-02"
    assert metadata["downloaded_at"].endswith("Z")
    assert metadata["content_length"] == len(
        json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    )
