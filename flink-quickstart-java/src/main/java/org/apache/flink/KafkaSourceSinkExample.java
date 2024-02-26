package org.apache.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.types.Row;

import java.util.Properties;

public class KafkaSourceSinkExample {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the Flink table environment
        // EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env); //, settings

        // Configure Kafka consumer properties
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "kafka-1:9092");
        consumerProps.setProperty("group.id", "flink-consumer-group");

        // Configure Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "kafka-1:9092");

        // Create a Kafka source
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "quote-data-raw",
                new SimpleStringSchema(),
                consumerProps
        );

        // Create a Kafka sink
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "quote-data-enriched",
                new SimpleStringSchema(),
                producerProps
        );

        // Register the PostgreSQL table with Flink SQL API
        String ddl = "CREATE TABLE postgresTable (\n" +
                "    key INT,\n" +
                "    symbol STRING,\n" +
                "    name STRING\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:postgresql://postgres:5432/postgres',\n" +
                "   'table-name' = 'quote_data_symbols',\n" +
                "   'username' = 'postgres',\n" +
                "   'password' = 'postgres'\n" +
                ")";
        tableEnv.executeSql(ddl);

        // Set up the Flink job
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Write the original Kafka stream to the Kafka sink without any transformation
        kafkaStream.addSink(kafkaProducer);

        // Query from the PostgreSQL table using Flink SQL
        Table resultTable = tableEnv.sqlQuery("SELECT * FROM postgresTable");

        // Print the result to the console (you can replace this with your desired sink)
        tableEnv.toRetractStream(resultTable, Row.class).print();

        // Execute the Flink job
        env.execute("Kafka Source Sink Example");
    }
}
