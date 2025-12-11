from typing import Optional
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import StreamingResponse
import uvicorn
import duckdb
import random
import string
from models import Catalog, DbSchema, Table, TableType, GlideEndpoint, GlideInfo, StatementQuery
from utils import generate_bytes, duckdb_to_arrow_schema, substrait_to_arrow_schema
import boto3
from botocore.client import Config

conn = duckdb.connect()
conn.execute("INSTALL tpch")
conn.execute("LOAD tpch")
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")
conn.execute("INSTALL substrait from community")
conn.execute("LOAD substrait")
conn.execute("CALL dbgen(sf=0.01)")

bucket_name = "glider"
endpoint_url = 'minio:9000'
aws_access_key_id = 'minioadmin'
aws_secret_access_key = 'minioadmin'
protocol = 'http'

conn.sql(
f"""
CREATE SECRET (
    TYPE s3,
    KEY_ID '{aws_access_key_id}',
    SECRET '{aws_secret_access_key}',
    REGION 'us-east-1',
    ENDPOINT '{endpoint_url}',
    USE_SSL '{'false' if protocol == 'http' else 'true'}',
    URL_STYLE 'path'
);
"""
)

app = FastAPI()

glide_store = {}
arrow_store = {}
schema_store = {} ## this is unnecessary

s3_client = boto3.client(
    's3',
    endpoint_url=f'{protocol}://{endpoint_url}',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

try:
    s3_client.head_bucket(Bucket=bucket_name)
except:
    s3_client.create_bucket(Bucket=bucket_name)

@app.get("/catalogs", response_model=list[Catalog])
def catalogs():
    return [
        Catalog(catalog_name="default")
    ]

@app.get("/db_schemas", response_model=list[DbSchema])
def db_schemas(catalog: Optional[str] = "", db_schema_filter_pattern: Optional[str] = ""):    
    return [
        DbSchema(catalog_name="default", db_schema_name=db[0])
        for db in conn.sql("SHOW DATABASES").fetchall()
    ]

@app.get("/tables", response_model=list[Table])
def tables(catalog: Optional[str] = "", db_schema_filter_pattern: Optional[str] = "", table_name_filter_pattern: Optional[str] = ""):
    return [
        Table(catalog_name=t[0], 
              db_schema_name=t[1], 
              table_name=t[2], 
              table_type=t[3], 
              table_schema=duckdb_to_arrow_schema(conn, f"SELECT * FROM {t[2]}").serialize().to_pybytes().hex())
        for t in conn.sql(f"""
                          SELECT table_catalog, table_schema, table_name, table_type 
                          FROM information_schema.tables
                          """).fetchall()
        if not table_name_filter_pattern or t[2] == table_name_filter_pattern
    ]



#   repeated string table_types = 4;
#   bool include_schema = 5;

@app.get("/table_types", response_model=list[TableType])
def table_types():
    return [
        TableType(table_type="internal")
    ]

def run_statement(statement: StatementQuery, handle: str, schema):
    if statement.sql:
        quack = conn.sql(statement.sql)
    else:
        quack = conn.sql("CALL from_substrait(?)", params=[bytes.fromhex(statement.substrait)])

    if statement.preferred_format == "application/vnd.apache.arrow.stream":
        arrow_store[handle] = (schema, quack.fetch_arrow_reader())
        location = ""
    elif statement.preferred_format == "application/vnd.apache.parquet":
        object_key = f'{handle}.parquet'
        quack.write_parquet(f"s3://{bucket_name}/{object_key}")
        location = s3_client.generate_presigned_url('get_object', Params={'Bucket': bucket_name, 'Key': object_key}, ExpiresIn=3600)
    else:
        raise Exception(f'Unknown format {statement.preferred_format}')

    
    
    gi: GlideInfo = glide_store[handle]
    gi.status = "completed"
    gi.endpoints.append(GlideEndpoint(ticket=handle, locations=[location]))



@app.post("/get_glide_info", response_model=GlideInfo)
def get_flight_info(statement: StatementQuery, bt: BackgroundTasks):
    handle = ''.join(random.choices(string.ascii_lowercase, k=10))

    if statement.handle:
        return glide_store[statement.handle]
    
    if statement.sql:
        schema = duckdb_to_arrow_schema(conn, statement.sql)
    else:
        schema = substrait_to_arrow_schema(statement.substrait)

    glide_info = GlideInfo(
        handle=handle,
        status="in-progress",
        endpoints=[] # TODO Is there an actual reason why ticket needs to be provided separately??
    )

    glide_store[handle] = glide_info

    # bt.add_task(run_statement, statement, handle, schema)

    run_statement(statement, handle, schema)
    
    return glide_info


@app.get("/get_stream")
def get_stream(ticket: str):
    stored_execution = arrow_store[ticket]
    return StreamingResponse(
        generate_bytes(stored_execution[0], stored_execution[1]),
        media_type="application/vnd.apache.arrow.stream"
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)


