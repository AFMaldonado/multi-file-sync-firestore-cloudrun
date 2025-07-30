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

class FechaRequest(BaseModel):
    fecha: str

@app.post("/")
async def procesar_archivos(body: FechaRequest):
    fecha = body.fecha
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    ventas_blob = bucket.blob(f"ventas_{fecha}.csv")
    clientes_blob = bucket.blob(f"clientes_{fecha}.csv")

    if not ventas_blob.exists() or not clientes_blob.exists():
        return {"error": f"Faltan archivos para la fecha {fecha}"}

    ventas_df = pd.read_csv(BytesIO(ventas_blob.download_as_bytes()))
    clientes_df = pd.read_csv(BytesIO(clientes_blob.download_as_bytes()))

    df = pd.merge(ventas_df, clientes_df, on="id_cliente", how="inner")

    bq_client = bigquery.Client()
    table_id = f"{bq_client.project}.{BQ_DATASET}.{BQ_TABLE}"
    job = bq_client.load_table_from_dataframe(df, table_id)
    job.result()

    return {"mensaje": f"Archivos de {fecha} procesados y cargados a BigQuery"}
