from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.table.table_environment import StreamTableEnvironment

# Create a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
# Create a TableEnvironment
t_env = StreamTableEnvironment.create(env)


t_env.execute_sql("""
    CREATE TABLE messages_gen (
        message_id INT,
        message_body STRING,
        message_time STRING,
        message_level INT
    ) WITH (
        'connector' = 'datagen',
        'fields.message_id.kind' = 'sequence',
        'fields.message_id.start' = '1',
        'fields.message_id.end' = '1000',
        'number-of-rows' = '1000',
        'fields.message_body.length' = '50',
        'fields.message_body.kind' = 'random',
        'rows-per-second' = '10',
        'fields.message_level.min' = '1',
        'fields.message_level.max' = '2'
    );
""")

# Conversion between Table and DataStream
ds = t_env.to_append_stream(
        t_env.from_path('messages_gen'),
            Types.ROW_NAMED(['message_id', 'message_body', 'message_time', 'message_level'],
                                [Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])
    )




# Filter records
#filtered_ds = ds.filter(lambda value: value['message_level'] == 1)
filtered_ds = ds.filter(lambda value: print(value) or int(value['message_level']) == 1)



# Define the schema for Kafka
row_type_info = Types.ROW_NAMED(['message_id', 'message_body', 'message_time', 'message_level'],
                                [Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])
json_serialization_schema = JsonRowSerializationSchema.builder().with_type_info(row_type_info).build()

sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka-kafka-bootstrap:9092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("messages")
            .set_value_serialization_schema(json_serialization_schema)
            .build()
    ) \
    .build()

# Add KafkaSink as a sink to the environment
filtered_ds.sink_to(sink)


# Execute the job
env.execute()
