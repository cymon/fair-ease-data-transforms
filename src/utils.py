import io
import json

import pandas as pd
from minio import Minio
from minio.error import S3Error

with open("../credentials.json") as f:
    _creds = json.load(f)

client = Minio(
    "10.4.1.4:9000",
    secure=False,
    access_key=_creds["accessKey"],
    secret_key=_creds["secretKey"],
)


def get_object(bucket_name, file_format, file_name, verbose=True):
    if verbose:
        print(f"{bucket_name=} - {file_format=} - {file_name=}")
    try:
        response = client.get_object(bucket_name, file_name)
        buffer = io.BytesIO(response.read())
    except S3Error:
        raise
    finally:
        if file_format == "parquet":
            df = pd.read_parquet(buffer, engine="pyarrow")
        elif file_format == "csv":
            df = pd.read_csv(buffer)
        else:
            raise ValueError(f"Unknown {file_format=}")
        response.close()
        response.release_conn()
        if verbose:
            print(f"Downloaded {file_name} into dataframe")
    return df
