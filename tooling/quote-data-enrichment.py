from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
from pyflink.datastream.formats.avro import AvroRowDeserializationSchema, AvroRowSerializationSchema
from avro.schema import Parse

# Create a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

def read_avro_schema_from_file(file_path):
    with open(file_path, 'r') as schema_file:
        avro_schema_str = schema_file.read()
        avro_schema = Parse(avro_schema_str)
        return avro_schema

# Replace 'your_schema.avsc' with the path to your Avro schema file
avro_schema_file_path = 'QuoteData.avsc'

avro_schema = read_avro_schema_from_file(avro_schema_file_path)
print(avro_schema)

# Define the KafkaSource using the builder syntax
source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka-1:9092,kafka-2:9092,kafka-3:9092") \
    .set_topics("quote-data-raw") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(AvroRowDeserializationSchema(avro_schema_string=avro_schema)) \
    .build()

# Add KafkaSource as a source to the environment
ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka-kafka-bootstrap:9095") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("alerts-sink")
            .set_value_serialization_schema(AvroRowSerializationSchema(avro_schema_string=avro_schema))
            .build()
    ) \
    .build()

# Add KafkaSink as a sink to the environment
ds.sink_to(sink)

# Execute the job
env.execute()
