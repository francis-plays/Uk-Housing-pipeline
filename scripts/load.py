import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import pandas as pd
import snowflake.connector
from io import StringIO
from snowflake.connector.pandas_tools import write_pandas
from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_BUCKET_NAME,
    AWS_REGION,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE
)


def pull_from_s3():
    """Connect to S3 and pull the latest raw housing CSV"""
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix="raw/")
    files = response.get("Contents", [])

    if not files:
        print("No raw files found in S3.")
        return None

    latest = sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]
    print(f"Pulling latest file: {latest['Key']}")

    obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=latest["Key"])
    df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
    print(f"Loaded {len(df)} rows from S3")
    return df


def connect_to_snowflake():
    """Connect to Snowflake"""
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database="uk_housing",
        schema="raw",
        warehouse=SNOWFLAKE_WAREHOUSE
    )
    print("Connected to Snowflake.")
    return conn


def load_to_snowflake(conn, df):
    """Bulk insert entire dataframe into Snowflake in one operation"""
    # Snowflake expects uppercase column names
    df.columns = [col.upper() for col in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="RAW_HOUSING",
        database="UK_HOUSING",
        schema="RAW"
    )

    if success:
        print(f"Successfully inserted {nrows} rows in {nchunks} chunks.")
    else:
        print("Load failed.")


def run():
    df = pull_from_s3()
    if df is not None:
        conn = connect_to_snowflake()
        load_to_snowflake(conn, df)
        conn.close()
        print("Load complete. Connection closed.")


if __name__ == "__main__":
    run()