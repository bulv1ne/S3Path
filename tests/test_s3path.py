import pytest

from s3path.s3path import S3Path


def test_read(mocked_s3_bucket_name):
    S3Path(f"s3://{mocked_s3_bucket_name}/mykey.txt").write_text("contents of file")


def test_str():
    raw_path = "s3://bucket/mykey.txt"
    path = S3Path(raw_path)
    assert str(path) == raw_path
    assert repr(path) == f"<S3Path ({raw_path})>"


def test_equal():
    raw_path1 = "s3://bucket/dir/mykey.txt"
    raw_path2 = "/bucket/dir/mykey.txt"
    raw_path3 = "s3a://bucket/dir/mykey.txt"
    assert S3Path(raw_path1) == S3Path(raw_path2)
    assert S3Path(raw_path1) == S3Path(raw_path3)

    assert S3Path(raw_path1) == S3Path("s3://bucket/dir") / "mykey.txt"
    assert S3Path(raw_path1) == S3Path("s3://bucket/dir") / "/mykey.txt"
    assert S3Path(raw_path1) == S3Path("s3://bucket/dir/") / "mykey.txt"
    assert S3Path(raw_path1) == S3Path(raw_path1) / ""

    assert S3Path(raw_path1) != raw_path1


def test_copy(mocked_s3_bucket_name):
    target_file1 = S3Path(f"s3://{mocked_s3_bucket_name}/mykey1.txt")
    target_file2 = S3Path(f"s3://{mocked_s3_bucket_name}/mykey2.txt")
    target_path3 = f"s3://{mocked_s3_bucket_name}/mykey3.txt"

    target_file1.write_text("contents")
    target_file1.copy(target_file2)
    target_file1.copy(target_path3)

    assert target_file2.read_text() == "contents"
    assert S3Path(target_path3).read_text() == "contents"


def test_delete(mocked_s3_bucket_name):
    target_file = S3Path(f"s3://{mocked_s3_bucket_name}/mykey1.txt")
    target_file.write_text("contents")
    target_file.delete()
    with pytest.raises(Exception):
        target_file.read_text()


def test_file_not_found(mocked_s3_bucket_name):
    with pytest.raises(FileNotFoundError):
        S3Path(f"s3://{mocked_s3_bucket_name}/mykey.txt").read_text()


def test_glob(mocked_s3_bucket_name):
    target_file = S3Path(f"s3://{mocked_s3_bucket_name}/mykey.txt")
    target_file.write_text("contents of file")
    assert list(S3Path(f"s3://{mocked_s3_bucket_name}").glob()) == [target_file]
