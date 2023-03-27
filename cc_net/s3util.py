import re
import io
from typing import Tuple
from pathlib import Path

import boto3
import botocore

s3_client = boto3.client("s3")

class RetryableDownloadFailure(Exception):
    def __init__(self, err: Exception):
        self.err = err

def try_get_content(url: str) -> bytes:
    bucket,key = _parse_url(url)
    try:
        buffer = io.BytesIO()
        s3_client.download_fileobj(Bucket=bucket,
                                   Key=key,
                                   Fileobj=buffer,
                                   Config=boto3.s3.transfer.TransferConfig(use_threads=False))
    except botocore.exceptions.ClientError as e:
        message = e.args[0] if isinstance(e.args[0], str) else ""
        if not "SlowDown" in message:
            raise e
        raise RetryableDownloadFailure(e)
    return buffer.getvalue()

def exists(url: str) -> bool:
    bucket,key = _parse_url(url)
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            return False

def upload(path: Path, url: str):
    if path.is_dir():
        for f in path.iterdir():
            upload(f, f"{url}/{f.name}")
        return
    bucket, key = _parse_url(url)
    with open(path, "rb") as data:
        s3_client.upload_fileobj(data, bucket, key)

def download(url: str, path: Path):
    bucket,key = _parse_url(url)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as file:
        s3_client.download_fileobj(bucket, key, file)


def _parse_url(url: str) -> Tuple[str,str]:
    path_pattern = re.search("s3://([^/]*)/(.*)", url)
    bucket = path_pattern.group(1)
    prefix = path_pattern.group(2)
    return bucket, prefix


