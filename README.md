
## Glide SQL

Glide SQL is an http-based protocol heavily modeled after Arrow Flight and Arrow Flight SQL. Arrow project already contains recommendations and best practices for serving arrow over http, but there's no stable protocol with sql semantics that different clients can depend on. The goal of this project is to build a flight-sql like http api and a client-side adbc driver. This is not meant to be a library, just a reference implementation and hopefully a spec.

### Design Decisions

* Metadata Operations - Unlike Flight SQL, metadata requests are eager and responses are json rather than arrow. The assumption is that the performance should not be a factor unless working with a huge database.
* Data Operations - flow of client-server interaction are similar to flight. All requests other than the final calls to get_stream are json, while content-type of the final calls should be configurable. It can be arrow.stream, parquet or something else.
* Type Information - type information returned for a table or a query will be in some json representation of arrow schema. (?) Maybe it should also be configurable to use substrait instead of arrow. Current implementation return hex of serialized arrow schema.

## Glide SQL API Specification (so far)

A REST API for querying databases with Arrow streaming support.

### Metadata Endpoints (GET)

- `/catalogs` - List available catalogs
- `/db_schemas` - List schemas, optionally filtered by catalog and pattern
- `/tables` - List tables with schema metadata (serialized as hex), optionally filtered by catalog, schema, and table name patterns
- `/table_types` - List supported table types

### Query Endpoints

#### POST `/get_glide_info`
Submit a query (SQL or Substrait plan) and receive a ticket for streaming results.

**Request Body:**
```json
{
  "query": "string",
  "substrait": "string (hex)",
  "preferred_format": "string"
}
```

**Response:**
```json
{
  "endpoints": [
    {
      "ticket": "string",
      "locations": ["string"]
    }
  ]
}
```

#### GET `/get_stream?ticket={ticket}`
Stream query results as Arrow IPC format.

**Response:** `application/vnd.apache.arrow.stream`

### Query Flow

1. POST query to `/get_glide_info` → receive ticket
2. GET `/get_stream?ticket={ticket}` → receive Arrow stream

### Schema Encoding

- Table schemas: hex-encoded Arrow IPC format
- Substrait plans: hex-encoded binary format

## Example Usage

### List Tables
```
import requests

server_url = 'http://localhost:8000'
requests.get(f'{server_url}/tables').json()
```
```
[
    {
        'catalog_name': 'default',
        'db_schema_name': 'memory',
        'table_name': 'customer',
        'table_type': 'internal'
    },
]
```
### Run SQL Query
```
info = requests.post(f'{server_url}/get_glide_info', json={'query': 'SELECT c_custkey FROM customer'}).json()

batches = []

for endpoint in info['endpoints']:
    location = endpoint['locations'][0]
    location = f'{server_url}/get_stream' if not location else location
    ticket = endpoint['ticket']
    url = f'{location}?ticket={ticket}'
    response = requests.get(url)

    with pa.ipc.open_stream(response.content) as reader:
        schema = reader.schema
        try:
            while True:
                batches.append(reader.read_next_batch())
        except StopIteration:
            pass

pa.Table.from_batches(batches).to_pandas()
```
```
      c_custkey
0             1
1             2
2             3
3             4
4             5
...         ...
1495       1496
1496       1497
1497       1498
1498       1499
1499       1500

[1500 rows x 1 columns]
```

### Run Substrait Query
```
example in client.py
```
