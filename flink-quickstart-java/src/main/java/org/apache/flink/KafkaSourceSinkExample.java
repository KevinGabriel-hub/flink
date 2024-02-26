package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.types.Row;
import org.apache.flink.formats.json.JsonRowSerializationSchema;

import java.util.Properties;

public class KafkaSourceSinkExample {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the Flink table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // Define the SQL query for enrichment and filtering
        String query = "SELECT " +
                "  k.recordId, " +
                "  k.time, " +
                "  k.arrivalTime, " +
                "  k.bid0Price, " +
                "  k.bid0Ccy, " +
                "  k.bid0Size, " +
                "  k.bid0DataSource, " +
                "  k.bid0AgeOffsetMsecs, " +
                "  k.bid1Price, " +
                "  k.bid1Ccy, " +
                "  k.bid1Size, " +
                "  k.bid1DataSource, " +
                "  k.bid1AgeOffsetMsecs, " +
                "  k.bid2Price, " +
                "  k.bid2Ccy, " +
                "  k.bid2Size, " +
                "  k.bid2DataSource, " +
                "  k.bid2AgeOffsetMsecs, " +
                "  k.bid3Price, " +
                "  k.bid3Ccy, " +
                "  k.bid3Size, " +
                "  k.bid3DataSource, " +
                "  k.bid3AgeOffsetMsecs, " +
                "  k.bid4Price, " +
                "  k.bid4Ccy, " +
                "  k.bid4Size, " +
                "  k.bid4DataSource, " +
                "  k.bid4AgeOffsetMsecs, " +
                "  k.ask0Price, " +
                "  k.ask0Ccy, " +
                "  k.ask0Size, " +
                "  k.ask0DataSource, " +
                "  k.ask0AgeOffsetMsecs, " +
                "  k.ask1Price, " +
                "  k.ask1Ccy, " +
                "  k.ask1Size, " +
                "  k.ask1DataSource, " +
                "  k.ask1AgeOffsetMsecs, " +
                "  k.ask2Price, " +
                "  k.ask2Ccy, " +
                "  k.ask2Size, " +
                "  k.ask2DataSource, " +
                "  k.ask2AgeOffsetMsecs, " +
                "  k.ask3Price, " +
                "  k.ask3Ccy, " +
                "  k.ask3Size, " +
                "  k.ask3DataSource, " +
                "  k.ask3AgeOffsetMsecs, " +
                "  k.ask4Price, " +
                "  k.ask4Ccy, " +
                "  k.ask4Size, " +
                "  k.ask4DataSource, " +
                "  k.ask4AgeOffsetMsecs, " +
                "  p.symbol AS recordSymbol, " +
                "  p.name AS recordName " +
                "FROM " +
                "  kafkaStream AS k " +
                "JOIN " +
                "  postgresTable FOR SYSTEM_TIME AS OF k.proctime " +
                "ON " +
                "  k.recordId = p.key " +
                "WHERE " +
                "  k.ask0Price IS NOT NULL AND k.ask0Price != 0.0 AND " +
                "  k.bid0Price IS NOT NULL AND k.bid0Price != 0.0";

        // Execute the query and write the result to the Kafka sink
        Table resultTable = tableEnv.sqlQuery(query);

        // Create a Kafka sink
        FlinkKafkaProducer<Row> kafkaProducer = new FlinkKafkaProducer<>(
                "quote-data-enriched",
                (KeyedSerializationSchema) new JsonRowSerializationSchema.Builder(resultTable.getSchema().toRowType()).build(), // Use this constructor
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        // Convert the Table to a retract stream of Row
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);

        // Extract the Row from the Tuple2<Boolean, Row> and add the Kafka sink
        resultStream
                .filter(tuple -> tuple.f0)
                .map(tuple -> tuple.f1) // Extract Row from Tuple2<Boolean, Row>
                .addSink(kafkaProducer);

        // Execute the Flink job
        env.execute("Kafka Enrichment Example");
    }
}
