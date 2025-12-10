from typing import Optional
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import uvicorn
import duckdb
import random
import string
from models import Catalog, DbSchema, Table, TableType, GlideEndpoint, GlideInfo, StatementQuery
from utils import generate_bytes, duckdb_to_arrow_schema, substrait_to_arrow_schema

conn = duckdb.connect()
conn.execute("INSTALL tpch")
conn.execute("LOAD tpch")
conn.execute("INSTALL substrait from community")
conn.execute("LOAD substrait")
conn.execute("CALL dbgen(sf=0.01)")

app = FastAPI()

glide_store = {}

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

@app.post("/get_glide_info", response_model=GlideInfo)
def get_flight_info(statement: StatementQuery):
    ticket = ''.join(random.choices(string.ascii_lowercase, k=10))


    # TODO this should probably run in the background
    if statement.query:
        schema = duckdb_to_arrow_schema(conn, statement.query)
        reader = conn.sql(statement.query).fetch_arrow_reader()
    else:
        schema = substrait_to_arrow_schema(statement.substrait)
        reader = conn.sql("CALL from_substrait(?)", params=[bytes.fromhex(statement.substrait)]).fetch_arrow_reader()
        

    glide_store[ticket] = (schema, reader)

    return GlideInfo(
        endpoints=[GlideEndpoint(ticket=ticket, locations=[""])] # TODO Is there an actual reason why ticket needs to be provided separately??
    )


@app.get("/get_stream")
def get_stream(ticket: str):
    stored_execution = glide_store[ticket]
    return StreamingResponse(
        generate_bytes(stored_execution[0], stored_execution[1]),
        media_type="application/vnd.apache.arrow.stream"
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)


