package org.apache.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class KafkaSourceSinkExample {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka consumer properties
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "kafka-0:9092");
        consumerProps.setProperty("group.id", "flink-consumer-group");

        // Configure Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "kafka-0:9092");

        // Create a Kafka source
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "flink",
                new SimpleStringSchema(),
                consumerProps
        );

        // Create a Kafka sink
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "sink",
                new SimpleStringSchema(),
                producerProps
        );

        // Set up the Flink job
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Write the original stream to the Kafka sink without any transformation
        kafkaStream.addSink(kafkaProducer);

        // Execute the Flink job
        env.execute("Kafka Source Sink Example");
    }
}
