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

class GlideEndpoint(BaseModel):
    ticket: str
    locations: list[str]

class GlideInfo(BaseModel):
    # schema
    endpoints: list[GlideEndpoint]

class StatementQuery(BaseModel):
    query: Optional[str] = ''
    substrait: Optional[str] = ''
    preferred_format: Optional[str] = 'application/vnd.apache.arrow.stream'
