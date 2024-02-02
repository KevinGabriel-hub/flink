from pyflink.table.table_environment import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, Row
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema

# Create a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
# Create a TableEnvironment
t_env = StreamTableEnvironment.create(env)

# Define the schema for Kafka
row_type_info = Types.ROW_NAMED(['user_id', 'item_id', 'category_id', 'behavior', 'ts'],
                                [Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.STRING()])
t_env.execute_sql("""
    CREATE TABLE my_table (
        user_id INT,
        item_id INT,
        category_id INT,
        behavior STRING,
        ts STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'source',
        'scan.startup.mode' = 'earliest-offset',
        'properties.bootstrap.servers' = 'kafka-kafka-bootstrap:9095',
        'format' = 'json',
        'json.fail-on-missing-field' = 'true'
    );
""")

# Conversion between Table and DataStream
ds = t_env.to_append_stream(t_env.from_path('my_table'), row_type_info)

ds_transformed = ds.map(
    lambda value: Row(
        value[0],
        value[1],
        value[2],
        "JOB 6 " + value[3].upper() if value[3] is not None else "JOB 6",
        value[4]
    ),
    output_type=row_type_info
)
json_serialization_schema = JsonRowSerializationSchema.builder().with_type_info(row_type_info).build()

sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka-kafka-bootstrap:9095") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("sink")
            .set_value_serialization_schema(json_serialization_schema)
            .build()
    ) \
    .build()

# Add KafkaSink as a sink to the environment
ds_transformed.sink_to(sink)

# Execute the job
env.execute()