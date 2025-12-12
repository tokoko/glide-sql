import pyarrow as pa
import requests
import time

server_url = 'http://127.0.0.1:8000'

def run_query(payload, fetch_metadata=False):
    def process_arrow_stream(arrow_stream, batches):
        with pa.ipc.open_stream(arrow_stream) as reader:
            try:
                while True:
                    batches.append(reader.read_next_batch())
            except StopIteration:
                pass

    def process_result_set(result_set, batches, seen_endpoints = 0):
        for endpoint in result_set['endpoints'][seen_endpoints:]:
            location = endpoint['locations'][0]
            ## HANDLE PARQUET RESPONSE
            if payload["preferred_format"] == "application/vnd.apache.parquet": #TODO this should rely on actual format, not preferred_format
                import pyarrow.parquet as pq
                import io
                response = requests.get(location, stream=True)
                parquet_file = pq.ParquetFile(io.BytesIO(response.content))

                for batch in parquet_file.iter_batches(batch_size=1024):
                    batches.append(batch)
            ## HANDLE ARROW.STREAM RESPONSE
            else:
                location = f'{server_url}/get_stream' if not location else location
                url = f'{location}?ticket={endpoint["ticket"]}' # TODO Do we neeed a separate ticket concept? Why not just sign a url
                process_arrow_stream(requests.get(url, stream=True).content, batches)

        return len(result_set['endpoints'])

    start = time.time()

    ## INITIAL QUERY REQUEST
    response = requests.post(f'{server_url}/query', json=payload)

    batches = []
    ## DIRECT FLOW ACTIVATED
    if response.headers["content-type"] == "application/vnd.apache.arrow.stream":
        handle = response.headers["X-Glide-Query-Handle"]
        process_arrow_stream(response.content, batches)
        if fetch_metadata:
            requests.get(f'{server_url}/result_set/{handle}', stream=True).json()
    ## INDIRECT FLOW ACTIVATED
    elif response.headers["content-type"] == "application/json":
        result_set = response.json()
        seen_endpoints = process_result_set(result_set, batches)

        while result_set["status"] != "completed":
            result_set = requests.get(f'{server_url}/result_set/{result_set["handle"]}').json()
            seen_endpoints = process_result_set(result_set, batches, seen_endpoints)
    else:
        raise Exception("Unknown content-type")
    
    end = time.time()
    table = pa.Table.from_batches(batches)
    print(f"Query took {end - start}")
    return table



def benchmark(query):
    print(f"============== {query} ==============")
    print("Object Storage - ", end="")
    run_query({'query': query, 'query_type': 'sql', 'allow_direct': False, 'preferred_format': 'application/vnd.apache.parquet'})
    print("Indirect flow - ", end="")
    run_query({'query': query, 'query_type': 'sql', 'allow_direct': False, 'preferred_format': 'application/vnd.apache.arrow.stream'})
    print("Direct flow w/o metadata - ", end="")
    run_query({'query': query, 'query_type': 'sql', 'allow_direct': True})
    print("Direct flow with metadata - ", end="")
    run_query({'query': query, 'query_type': 'sql', 'allow_direct': True}, fetch_metadata=True)
    

benchmark('SELECT * FROM customer')
benchmark('SELECT COUNT(*) FROM customer')


# prepared = requests.post(f'{server_url}/prepared_statement', json={'query': 'SELECT c_custkey FROM customer'}).json()

# print(run_query(prepared['handle'], 'application/vnd.apache.parquet').to_pandas())


exit(0)

# pprint(requests.get(f'{server_url}/tables').json())

# pprint(requests.get(f'{server_url}/db_schemas').json())

print(run_query('SELECT c_custkey FROM customer', preferred_format='application/vnd.apache.parquet').to_pandas())

print(run_query('SELECT c_custkey FROM customer', preferred_format='application/vnd.apache.arrow.stream').to_pandas())

exit(0)




# """
# Substrait Query
# """
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
print(run_query(table, preferred_format='application/vnd.apache.parquet').to_pandas())
