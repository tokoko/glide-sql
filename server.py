from typing import Optional
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import StreamingResponse
import uvicorn
import duckdb
import random
import string
from models import Catalog, DbSchema, Table, TableType, ResultEndpoint, ResultSet, Query, PreparedStatement
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
conn.execute("CALL dbgen(sf=4)")

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

def execute_query(query: str, query_type: str):
    if query_type == "sql":
        return conn.sql(query)
    elif query_type == "sql":
        return conn.sql("CALL from_substrait(?)", params=[bytes.fromhex(query)])
    elif query_type == "prepared_statement":
        return conn.sql(f"EXECUTE {query}")
    else:
        raise Exception(f"Unknown query_type {query_type}")
    

def run_statement(statement: Query, handle: str, schema):
    quack = execute_query(statement.query, statement.query_type)

    if statement.preferred_format == "application/vnd.apache.arrow.stream":
        arrow_store[handle] = (schema, quack.fetch_arrow_reader())
        location = ""
    elif statement.preferred_format == "application/vnd.apache.parquet":
        object_key = f'{handle}.parquet'
        quack.write_parquet(f"s3://{bucket_name}/{object_key}")
        location = s3_client.generate_presigned_url('get_object', Params={'Bucket': bucket_name, 'Key': object_key}, ExpiresIn=3600)
    else:
        raise Exception(f'Unknown format {statement.preferred_format}')
    
    gi: ResultSet = glide_store[handle]
    gi.status = "completed"
    gi.endpoints.append(ResultEndpoint(ticket=handle, locations=[location]))

### Prepared Statements

prepared_statements = {}

# should prepared statements be named?
@app.post("/prepared_statement", response_model=PreparedStatement)
def get_flight_info(prepared_statement: PreparedStatement):
    handle = ''.join(random.choices(string.ascii_lowercase, k=10))
    conn.sql(f"PREPARE {handle} AS {prepared_statement.query}")
    prepared_statement.handle = handle
    return prepared_statement

### Query

@app.get("/result_set/{handle}", response_model=ResultSet)
def result_set(handle: str):
    return glide_store[handle]
    
@app.post("/query", response_model=ResultSet)
def get_flight_info(statement: Query, bt: BackgroundTasks):
    handle = ''.join(random.choices(string.ascii_lowercase, k=10))

    if statement.query_type == "sql":
        schema = duckdb_to_arrow_schema(conn, statement.query)
    elif statement.query_type == "substrait":
        schema = substrait_to_arrow_schema(statement.query)
    elif statement.query_type == "prepared_statement":
        schema = conn.sql(f"EXECUTE {statement.prepared_statement_handle}").fetch_arrow_table().schema ## TODO
    else:
        raise Exception(f"Unknown query_type {statement.query}")
 
    result_set = ResultSet(
        handle=handle,
        status="in-progress",
        schema=schema.serialize().to_pybytes().hex(),
        endpoints=[] # TODO Is there an actual reason why ticket needs to be provided separately??
    )

    glide_store[handle] = result_set

    if statement.allow_direct:
        quack = execute_query(statement.query, statement.query_type)
        response = StreamingResponse(
            generate_bytes(schema, quack.fetch_arrow_reader()),
            media_type="application/vnd.apache.arrow.stream"
        )
        response.headers["X-Glide-Query-Handle"] = handle
        return response
    else:
        bt.add_task(run_statement, statement, handle, schema)
    
    return result_set


@app.get("/get_stream")
def get_stream(ticket: str):
    stored_execution = arrow_store[ticket]
    return StreamingResponse(
        generate_bytes(stored_execution[0], stored_execution[1]),
        media_type="application/vnd.apache.arrow.stream"
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)


