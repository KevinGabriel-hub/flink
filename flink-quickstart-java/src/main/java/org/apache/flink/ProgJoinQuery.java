package org.apache.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.model.Syslog;
import org.apache.flink.model.SyslogEnriched;
import org.apache.flink.schema.SyslogDeserializationSchema;
import org.apache.flink.schema.SyslogEnrichedSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProgJoinQuery {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        KafkaSource<Syslog> source1 = KafkaSource.<Syslog>builder()
                .setBootstrapServers("kafka-1:9092")
                .setTopics("source")
                .setGroupId("group1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SyslogDeserializationSchema())
                .build();

        DataStream<Syslog> kafkaStream = env.fromSource(
                source1, WatermarkStrategy.noWatermarks(), "Kafka Source");

        tableEnv.createTemporaryView("syslog_raw", kafkaStream);

        // Register the MySQL table with Flink SQL API
        String create_mysql_table = "CREATE TABLE iptable (\n" +
                "    ip_address STRING,\n" +
                "    hostname STRING,\n" +
                "    spi STRING,\n" +
                "    network STRING,\n" +
                "    region STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://mysql:3306/flink',\n" +
                "    'table-name' = 'your_table_name',\n" +
                "    'username' = 'flink',\n" +
                "    'password' = 'password',\n" +
                "    'driver' = 'com.mysql.jdbc.Driver'\n" +
                ")";
        tableEnv.executeSql(create_mysql_table);

        Table result =
                tableEnv.sqlQuery(
                        "SELECT " +
                                "k.message_timestamp, " +
                                "k.hostname, " +
                                "k.app, " +
                                "k.ip_address, " +
                                "m.ip_address, " +
                                "m.hostname " +
                                "FROM " +
                                "syslog_raw AS k " +
                                "JOIN iptable AS m " +
                                "ON k.ip_address = m.ip_address"
                );

        DataStream<SyslogEnriched> syslogEnrichedTable = tableEnv.toDataStream(result,
                SyslogEnriched.class);

        KafkaSink<SyslogEnriched> sink = KafkaSink.<SyslogEnriched>builder()
                .setBootstrapServers("kafka-1:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("sink")
                        .setValueSerializationSchema(new SyslogEnrichedSerializationSchema())
                        .build()
                )//.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        syslogEnrichedTable.sinkTo(sink);

        env.execute("Flink Streaming Join Demo");
    }
}
