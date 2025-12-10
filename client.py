import pyarrow as pa
import requests
from pprint import pprint

server_url = 'http://localhost:8000'

pprint(requests.get(f'{server_url}/tables').json())

pprint(requests.get(f'{server_url}/db_schemas').json())

"""
SQL Query
"""
def run_query(query):
    if isinstance(query, str):
        payload = {'query': query}
    else:
        payload = {'substrait': query.SerializeToString().hex()}
    info = requests.post(f'{server_url}/get_glide_info', json=payload).json()

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
    
    return pa.Table.from_batches(batches)

print(run_query('SELECT c_custkey FROM customer').to_pandas())

"""
Substrait Query
"""
import pyarrow.substrait as pa_substrait
from substrait.builders.plan import read_named_table, filter
from substrait.builders.extended_expression import column, scalar_function, literal
from substrait.builders.type import i64
from substrait.extension_registry import ExtensionRegistry

def read_glide_sql_named_table(name: str):
    pa_schema_hex = requests.get(f'{server_url}/tables?table_name_filter_pattern={name}').json()[0]['table_schema']
    pa_schema = pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(pa_schema_hex)))
    substrait_schema = pa_substrait.serialize_schema(pa_schema).to_pysubstrait().base_schema
    return read_named_table(name, substrait_schema)

table = read_glide_sql_named_table('customer')

## c_custkey = 3
table = filter(
    table,
    expression=scalar_function(
        "extension:io.substrait:functions_comparison",
        "equal",
        expressions=[column("c_custkey"), literal(3, i64())],
    ),
)

table = table(ExtensionRegistry())
print(run_query(table).to_pandas())
