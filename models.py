from pydantic import BaseModel
from typing import Optional

class Catalog(BaseModel):
    catalog_name: str

class DbSchema(BaseModel):
    catalog_name: str
    db_schema_name: str

class Table(BaseModel):
    catalog_name: str
    db_schema_name: str
    table_name: str
    table_type: str
    table_schema: str

class TableType(BaseModel):
    table_type: str

class PreparedStatement(BaseModel):
    handle: Optional[str] = ""
    query: str

class ResultEndpoint(BaseModel):
    ticket: str
    locations: list[str]

class ResultSet(BaseModel):
    handle: str
    status: str
    schema: str
    endpoints: list[ResultEndpoint]

class QueryResult(BaseModel):
    handle: str
    status: str
    prepared_statement_handle: Optional[str]
    result_sets: list[ResultSet]

class Query(BaseModel):
    query: str
    query_type: str = "sql"
    prepare: bool = False
    allow_direct: bool = False
    preferred_format: Optional[str] = 'application/vnd.apache.arrow.stream'
#   result_set_expiration