from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer


# Load Avro schema
with open('QuoteData.avsc', 'r') as f:
    value_schema_str = f.read()
# Define a simple string schema for the key
key_schema_str = '{"type": "string"}'
schema_registry_conf = {
    'url': 'http://schema-registry:8081',
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
value_deserializer = AvroDeserializer(schema_registry_client, value_schema_str)
key_deserializer = AvroDeserializer(schema_registry_client, key_schema_str)
conf = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    'value.deserializer': value_deserializer,
    'key.deserializer': key_deserializer,
    'group.id': 'your_consumer_group_id',
    'auto.offset.reset': 'earliest',
}

consumer = DeserializingConsumer(conf)

# Subscribe to the Kafka topic
consumer.subscribe(['quote-data-raw'])


def process_logic(key, value):
    print("Received message - Key: {}, Value: {}".format(key, value))

try:
    while True:
        # Poll for messages
        msg = consumer.poll(1.0)  # Adjust the timeout as needed

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        # Process the consumed message
        key = msg.key()
        value = msg.value()

        # Your processing logic here
        process_logic(key, value)

except KeyboardInterrupt:
    pass
finally:
    # Close the consumer
    consumer.close()


"""
# Load Avro schema
with open('QuoteData.avsc', 'r') as f:
    value_schema_str = f.read()
# Define a simple string schema for the key
key_schema_str = '{"type": "string"}'
schema_registry_conf = {
    'url': 'http://schema-registry:8081', 
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
value_serializer = AvroSerializer(schema_registry_client, value_schema_str)
key_serializer = AvroSerializer(schema_registry_client, key_schema_str)
conf = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    'value.serializer': value_serializer,
    'key.serializer': key_serializer
}
producer = SerializingProducer(conf)


quote = generate_quote()
key = str(quote['recordId'])  # Convert recordId to string to use as key
producer.produce(topic='quote-data-raw', key=key, value=quote)
"""