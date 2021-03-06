from contextlib import contextmanager
from typing import Dict, Tuple, Type

from botocore.exceptions import ClientError

# Format: ExceptionClass, Message
_ERROR_MAP: Dict[str, Tuple[Type[Exception], str]] = {
    "NoSuchBucket": (
        FileNotFoundError,
        "The specified bucket {BucketName} does not exist",
    ),
    "NoSuchKey": (FileNotFoundError, "{Message}"),
    "404": (FileNotFoundError, "{Message}"),
}


@contextmanager
def handle_client_error():
    """Raises other exceptions depending on the error code.

    Converts the following codes to a different exception:
    - NoSuchBucket: FileNotFoundError
    - NoSuchKey: FileNotFoundError
    """
    try:
        yield
    except ClientError as client_error:
        error = client_error.response["Error"]
        if exception_msg := _ERROR_MAP.get(error["Code"]):
            exception_class, msg = exception_msg
            raise exception_class(msg.format(**error))
        raise client_error
