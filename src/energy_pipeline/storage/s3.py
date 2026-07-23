import json
import re
from datetime import UTC, date, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

from energy_pipeline.storage.raw import (
    RawObjectKey,
    raw_data_key,
    raw_metadata_key,
)

PARTITIONED_DATA_KEY_RE = re.compile(
    r"year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/data\.json$"
)


class S3RawStorage:
    def __init__(self, bucket: str, client: Any | None = None) -> None:
        self.bucket = bucket
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = boto3.client("s3")

        return self._client

    def exists(self, key: RawObjectKey) -> bool:
        try:
            self.client.head_object(
                Bucket=self.bucket,
                Key=raw_data_key(key),
            )
        except ClientError as error:
            if _client_error_code(error) == "404":
                return False
            raise

        return True

    def save_object(self, payload: dict, key: RawObjectKey) -> str:
        data_key = raw_data_key(key)
        metadata_key = raw_metadata_key(key)
        serialized_payload = _serialize_json(payload)

        self.client.put_object(
            Bucket=self.bucket,
            Key=data_key,
            Body=serialized_payload,
            ContentType="application/json",
        )
        self.client.put_object(
            Bucket=self.bucket,
            Key=metadata_key,
            Body=_serialize_json(
                {
                    "source": key.source,
                    "dataset": key.dataset,
                    "requested_date": key.date.isoformat(),
                    "downloaded_at": datetime.now(UTC).isoformat().replace(
                        "+00:00",
                        "Z",
                    ),
                    "content_length": len(serialized_payload.encode("utf-8")),
                }
            ),
            ContentType="application/json",
        )

        return self.data_uri(key)

    def data_uri(self, key: RawObjectKey) -> str:
        return f"s3://{self.bucket}/{raw_data_key(key)}"

    def latest_date(self, source: str, dataset: str) -> date | None:
        latest: date | None = None
        prefix = f"{source}/{dataset}/"
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                raw_date = _date_from_partitioned_key(item.get("Key", ""))
                if raw_date is None:
                    continue
                if latest is None or raw_date > latest:
                    latest = raw_date

        return latest


def _serialize_json(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _client_error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))


def _date_from_partitioned_key(key: str) -> date | None:
    match = PARTITIONED_DATA_KEY_RE.search(key)
    if match is None:
        return None

    try:
        return date(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        )
    except ValueError:
        return None
