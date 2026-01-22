import hashlib
import json
import os
from io import BytesIO, StringIO
from urllib.parse import urljoin

import boto3
import httpx
import pandas as pd
from bs4 import BeautifulSoup

ROOT_URL = os.environ.get("BASE_URL", "http://bls-app:5000")
BASE_PATH = "/pub/time.series/pr/"
BASE_URL = urljoin(ROOT_URL.rstrip("/") + "/", BASE_PATH.lstrip("/"))


def create_s3_bucket_if_not_exists(s3_client, bucket_name) -> None:
    """Create an S3 bucket if it does not already exist."""
    existing_buckets = s3_client.list_buckets()
    bucket_names = [bucket["Name"] for bucket in existing_buckets.get("Buckets", [])]

    if bucket_name not in bucket_names:
        print(f"Creating bucket: {bucket_name}")
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} created.")


def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def load_bls_file(filename: str) -> pd.DataFrame:
    """Download and load a BLS flat file into a DataFrame."""
    url = BASE_URL + filename
    r = httpx.get(url, timeout=30)
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text), sep="\t")

    # 🔑 CRITICAL FIX: remove hidden whitespace
    df.columns = df.columns.str.strip()

    return df


def list_bls_files() -> dict:
    r = httpx.get(BASE_URL, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    files = {}

    for link in soup.find_all("a"):
        href = link.get("href")
        print("ROOT_URL:", ROOT_URL)
        print("BASE_URL:", BASE_URL)
        print("Resolved:", urljoin(ROOT_URL, href))

        if not href or href.endswith("/") or href.startswith("?"):
            continue

        filename = os.path.basename(href)
        files[filename] = urljoin(ROOT_URL, href)

    return files


def list_bls_files_old() -> dict:
    """
    Returns:
        {filename: download_url}
    """
    r = httpx.get(BASE_URL, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    files = {}

    for link in soup.find_all("a"):
        href = link.get("href")

        # print("ROOT_URL:", ROOT_URL)
        # print("BASE_URL:", BASE_URL)
        # print("Resolved:", urljoin(ROOT_URL, href))

        if href and not href.endswith("/") and not href.startswith("?"):
            files[href] = BASE_URL + href

    return files


def list_s3_objects(s3_client, bucket: str, prefix: str) -> dict:
    """
    Returns:
        {filename: etag_without_quotes}
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    result = {}

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.replace(prefix, "", 1)
            result[filename] = obj["ETag"].strip('"')

    return result


def pull_population_data_to_s3(s3, bucket_name, prefix):
    """
    Fetch JSON data from API and upload it to S3 as a JSON file
    """
    API_URL = (
        "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
        "?cube=acs_yg_total_population_1"
        "&drilldowns=Year%2CNation"
        "&locale=en"
        "&measures=Population"
    )

    # Fetch data from API
    with httpx.Client(timeout=30.0) as client:
        response = client.get(API_URL)
        response.raise_for_status()
        data = response.json()

    # Create S3 object key (e.g., prefix/data_2026-01-21.json)
    # timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    s3_key = f"{prefix.rstrip('/')}/honolulu_population_data.json"

    # Upload to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json",
    )


def sync_bls_pr_to_s3(s3, bucket: str, prefix: str):
    """
    Sync BLS PR directory to S3.

    - Uploads new or changed files
    - Deletes removed files
    - Avoids duplicate uploads
    """

    bls_files = list_bls_files()
    s3_files = list_s3_objects(s3, bucket, prefix)

    # ---- Upload or update files ----
    for filename, url in bls_files.items():
        r = httpx.get(url, timeout=60)
        r.raise_for_status()
        content = r.content
        content_md5 = md5_bytes(content)

        if filename in s3_files and s3_files[filename] == content_md5:
            continue  # unchanged

        s3.put_object(Bucket=bucket, Key=prefix + filename, Body=BytesIO(content))

        print(f"Uploaded/Updated: {filename}")

    # ---- Delete removed files ----
    for filename in s3_files:
        if filename not in bls_files:
            s3.delete_object(Bucket=bucket, Key=prefix + filename)
            print(f"Deleted: {filename}")

    print("Sync complete.")


def handler(event, context):
    # 1. Initialize the S3 client using environment variables from docker-compose
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION") or "us-east-1",
    )

    bucket_name = event.get("bucket-name") or "data"
    create_s3_bucket_if_not_exists(s3, bucket_name)

    # file_list = []

    # try:
    #     # 2. List objects in the bucket
    #     response = s3.list_objects_v2(Bucket=bucket_name)

    #     if "Contents" in response:
    #         for obj in response["Contents"]:
    #             file_list.append(obj["Key"])

    #     message = f"Found {len(file_list)} files."
    # except Exception as e:
    #     message = f"Error: {str(e)}"

    # return {
    #     "statusCode": 200,
    #     "body": json.dumps({"message": message, "files": file_list}),
    #     "content-type": "application/json",
    # }

    try:
        pull_population_data_to_s3(s3, bucket_name, prefix="population")
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": f"Error pulling population data: {str(e)}"}),
        }

    try:
        sync_bls_pr_to_s3(s3, bucket_name, prefix=BASE_PATH.lstrip("/"))
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": f"Error syncing BLS PR data: {str(e)}"}),
        }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"message": "Sync completed."}),
    }
