#!/usr/bin/env python3
import os
import tempfile
import time

import boto3
from imagehash import phash
from PIL import Image


def _download_image_from_s3(bucket: str, key: str) -> str:
    s3 = boto3.client("s3")
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        s3.download_fileobj(bucket, key, tmp_file)
        return tmp_file.name


def _generate_phash(tmp_file_path: str):

    with Image.open(tmp_file_path) as img:
        start = time.process_time()
        img_phash = phash(img)
        elapsed = time.process_time() - start
        img_size = img.size

    # Prepare the response
    result = {
        "phash": str(img_phash),
        "image_size": img_size,
        "time_taken": elapsed,
    }
    return result


def lambda_handler(event, context):
    # Extract the S3 URL from the event
    bucket = event.get("bucket")

    allowed_buckets = os.getenv("ALLOWED_BUCKETS")
    if allowed_buckets:
        allowed_buckets = tuple(allowed_buckets.split(","))
    else:
        allowed_buckets = ("jiayu-test-phash",)

    # Check if the bucket is allowed
    if bucket != allowed_buckets:
        return {"error": f"Bucket {bucket} is not allowed"}

    key = event.get("key")
    if not key:
        return {"error": "No object/key provided"}

    # Download the image from S3
    tmp_file_path = _download_image_from_s3(bucket, key)
    result = _generate_phash(tmp_file_path)
    # Clean up the temporary file
    os.unlink(tmp_file_path)

    return result
