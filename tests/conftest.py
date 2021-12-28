import boto3
import pytest
from moto import mock_s3  # type: ignore


@pytest.fixture(scope="function")
def mocked_s3_bucket_name():
    with mock_s3():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="mybucket")
        yield "mybucket"
