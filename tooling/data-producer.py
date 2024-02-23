import time
import random

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# Load Avro schema
with open('QuoteData.avsc', 'r') as f:
    value_schema_str = f.read()

# Define a simple string schema for the key
key_schema_str = '{"type": "string"}'

# Kafka configuration


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

# Function to generate a random quote
def generate_quote():
    quote = {
        "recordId": random.randint(1, 50),
        "time": int(time.time()), # * 10**7),  # current time in 100-nanosecond units
        "arrivalTime": random.randint(int(time.time() - 10), int(time.time()))  # time between now and 10 seconds ago
    }
    # Generate bid and ask data
    last_bid_price = random.uniform(1, 100)
    last_ask_price = random.uniform(1, 100)
    # Random chance for bid and ask prices to be 0 or None
    bid_price_chance = random.uniform(0, 100)
    ask_price_chance = random.uniform(0, 100)

    if bid_price_chance <= 3:
        last_bid_price = 0
    elif bid_price_chance <= 6:
        last_bid_price = None

    if ask_price_chance <= 5:
        last_ask_price = 0
    elif ask_price_chance <= 10:
        last_ask_price = None

    for i in range(5):
        if last_bid_price is not None and last_bid_price != 0:
            last_bid_price += random.uniform(0.1, 10)
        if last_ask_price is not None and last_ask_price != 0:
            last_ask_price += random.uniform(0.1, 10)

        quote.update({
            f"bid{i}Price": last_bid_price,
            f"bid{i}Ccy": "USD",
            f"bid{i}Size": int(random.uniform(0.1, 100)),
            f"bid{i}DataSource": random.randint(1, 10),
            f"bid{i}AgeOffsetMsecs": None,
            f"ask{i}Price": last_ask_price,
            f"ask{i}Ccy": "USD",
            f"ask{i}Size": int(random.uniform(0.1, 100)),
            f"ask{i}DataSource": random.randint(1, 10),
            f"ask{i}AgeOffsetMsecs": None,
        })
    return quote

# Produce a message to the Kafka topic
import json

def produce():
    quote = generate_quote()
    key = str(quote['recordId'])  # Convert recordId to string to use as key
    producer.produce(topic='quote-data-raw', key=key, value=quote)

# Produce a quote every second
start_time = time.time()
message_count = 0
while True:
    produce()
    message_count += 1
    if time.time() - start_time >= 30:
        print(f"Sent {message_count} messages in the last 30 seconds")
        start_time = time.time()
        message_count = 0
    time.sleep(random.uniform(0.025, 0.075))
