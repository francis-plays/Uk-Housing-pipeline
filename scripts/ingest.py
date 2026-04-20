import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_BUCKET_NAME,
    AWS_REGION
)

# Step 1: Define column names
COLUMNS = [
    "transaction_id",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town",
    "district",
    "county",
    "ppd_category",
    "record_status"
]


def read_csv(filepath):
    """Step 2 & 3: Read CSV and take first 10,000 rows"""
    print(f"Reading file: {filepath}")
    df = pd.read_csv(
        filepath,
        header=None,        # file has no header row
        names=COLUMNS,      # apply our column names
        nrows=10000         # only load first 10,000 rows
    )
    print(f"Loaded {len(df)} rows with {len(df.columns)} columns")
    return df


def convert_to_csv(df):
    """Step 4: Convert dataframe to CSV in memory"""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    print("Converted to CSV in memory")
    return csv_buffer


def connect_to_s3():
    """Step 5: Connect to S3"""
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    print("Connected to S3.")
    return s3


def push_to_s3(s3, csv_buffer):
    """Step 6: Push CSV to S3 with timestamp"""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    key = f"raw/housing_{timestamp}.csv"

    s3.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )
    print(f"Raw data saved to S3: s3://{AWS_BUCKET_NAME}/{key}")
    return key


def run():
    # Update this path to where your downloaded file is
    filepath = "/Users/francis/Downloads/pp-2024.csv"

    df = read_csv(filepath)
    csv_buffer = convert_to_csv(df)
    s3 = connect_to_s3()
    push_to_s3(s3, csv_buffer)
    print("Ingest complete.")


if __name__ == "__main__":
    run()