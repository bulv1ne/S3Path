from typing import Iterator, Optional, Union

import boto3

from s3path.errors import handle_client_error


class S3Path:
    def __init__(self, path: str, client=None):
        """Path like interface for S3

        Examples for path values:
        - s3://bucket/path/to/file
        - s3a://bucket/path/to/file
        - /bucket/path/to/file
        - bucket/path/to/file
        """
        # Cleanup path
        if path.startswith("s3://"):
            path = path[5:]  # Remove "s3://"
        elif path.startswith("s3a://"):
            path = path[6:]  # Remove "s3a://"
        elif path.startswith("/"):
            path = path[1:]  # Remove "/"

        result = path.split("/", 1)
        self.bucket = result[0]
        if len(result) > 1:
            self.key = result[1]
        else:
            self.key = ""

        self._s3_client = client or boto3.client("s3")

    def __truediv__(self, other: Optional[str]) -> "S3Path":
        self_path = str(self)
        if not other:
            return S3Path(str(self))
        if other.startswith("/"):
            other = other[1:]
        if self_path.endswith("/"):
            return S3Path(f"{self_path}{other}", client=self._s3_client)
        return S3Path(f"{self_path}/{other}", client=self._s3_client)

    def __str__(self):
        return f"s3://{self.bucket}/{self.key}"

    def __repr__(self):
        return f"<S3Path ({self})>"

    def __eq__(self, other):
        if isinstance(other, S3Path):
            return self.bucket == other.bucket and self.key == other.key
        return False

    def read_bytes(self) -> bytes:
        with handle_client_error():
            s3_object = self._s3_client.get_object(Bucket=self.bucket, Key=self.key)
            return s3_object["Body"].read()

    def read_text(self, encoding="utf-8") -> str:
        return self.read_bytes().decode(encoding)

    def write_bytes(self, data: bytes):
        with handle_client_error():
            self._s3_client.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=data,
            )

    def write_text(self, data: str, encoding="utf-8"):
        self.write_bytes(data.encode(encoding))

    def glob(self, suffix: str = "") -> Iterator["S3Path"]:
        """Retrieve the keys from an s3 path with suffix

        Args:
            suffix (str): Only fetch keys that end with this suffix (optional).

        Returns:
            Iterator[S3Path]
        """

        paginator = self._s3_client.get_paginator("list_objects_v2")
        for resp in paginator.paginate(Bucket=self.bucket, Prefix=self.key):
            if "Contents" in resp:
                for obj in resp["Contents"]:
                    key = obj["Key"]
                    if not suffix or key.endswith(suffix):
                        yield S3Path(self.bucket, client=self._s3_client) / key

    def copy(self, target: Union[str, "S3Path"]) -> None:
        if not isinstance(target, S3Path):
            target = S3Path(target, client=self._s3_client)

        # Copy the file from the source key to the target key
        with handle_client_error():
            self._s3_client.copy(
                {"Bucket": self.bucket, "Key": self.key},
                target.bucket,
                target.key,
            )

    def delete(self) -> None:
        with handle_client_error():
            self._s3_client.delete_object(Bucket=self.bucket, Key=self.key)
