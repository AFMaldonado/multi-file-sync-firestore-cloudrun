from fastapi import FastAPI, Request
from pydantic import BaseModel
from google.cloud import storage, bigquery
import pandas as pd
from io import BytesIO
import os

app = FastAPI()

BUCKET_NAME = os.environ["BUCKET_NAME"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE = os.environ["BQ_TABLE"]

class DateRequest(BaseModel):
    date: str

@app.post("/")
def process_files(body: DateRequest):
    date = body.date
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    sales_blob = bucket.blob(f"sales_{date}.csv")
    customers_blob = bucket.blob(f"customers_{date}.csv")

    if not sales_blob.exists() or not customers_blob.exists():
        return {"error": f"Missing files for date {date}"}

    sales_df = pd.read_csv(BytesIO(sales_blob.download_as_bytes()))
    customers_df = pd.read_csv(BytesIO(customers_blob.download_as_bytes()))

    df = pd.merge(sales_df, customers_df, on="customer_id", how="inner")

    bq_client = bigquery.Client()
    table_id = f"{bq_client.project}.{BQ_DATASET}.{BQ_TABLE}"
    job = bq_client.load_table_from_dataframe(df, table_id)
    job.result()

    return {"message": f"Files from {date} processed and loaded into BigQuery"}
