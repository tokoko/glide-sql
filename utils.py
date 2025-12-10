import pyarrow as pa
import io
from substrait.gen.proto.plan_pb2 import Plan
from substrait.type_inference import infer_plan_schema
import pyarrow.substrait as pa_substrait

def generate_bytes(schema, batches):
    with pa.RecordBatchReader.from_batches(schema, batches) as source, \
            io.BytesIO() as sink, \
            pa.ipc.new_stream(sink, schema) as writer:
        for batch in source:
            sink.seek(0)
            writer.write_batch(batch)
            sink.truncate()
            with sink.getbuffer() as buffer:
                yield buffer

        sink.seek(0)
        writer.close()
        sink.truncate()
        with sink.getbuffer() as buffer:
            yield buffer

def duckdb_to_arrow_schema(conn, query):
    result = conn.execute(f"DESCRIBE {query}").fetchall()

    fields = []
    for row in result:
        column_name = row[0]
        column_type = row[1]
        
        # Map DuckDB types to Arrow types
        arrow_type = conn.execute(f"SELECT NULL::{column_type}").fetch_arrow_table().schema[0].type
        fields.append(pa.field(column_name, arrow_type))

    return pa.schema(fields)

def substrait_to_arrow_schema(plan):
    plan = Plan.FromString(bytes.fromhex(plan))
    substrait_schema = infer_plan_schema(plan)
    return pa_substrait.deserialize_schema(substrait_schema.SerializeToString())
