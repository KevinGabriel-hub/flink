package org.apache.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.model.SqlTable;
import org.apache.flink.model.Syslog;
import org.apache.flink.model.SyslogEnriched;
import org.apache.flink.schema.SyslogDeserializationSchema;
import org.apache.flink.schema.SyslogEnrichedSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;

public class ProgJoin {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Kafka Source for syslog
        KafkaSource<Syslog> kafkaSource = KafkaSource.<Syslog>builder()
                .setBootstrapServers("kafka-kafka-bootstrap:9095")
                .setTopics("syslog-generated")
                .setGroupId("group1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SyslogDeserializationSchema())
                .build();

        DataStream<Syslog> syslogStream = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

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

        // Convert the MySQL table to a DataStream
        Table mysqlTable = tableEnv.from("iptable");
        DataStream<SqlTable> sqlStream = tableEnv.toDataStream(mysqlTable, SqlTable.class);

        // Join operation between syslogStream and enrichedStream
        DataStream<SyslogEnriched> joinedStream = syslogStream
                .connect(sqlStream)
                .keyBy(Syslog::getIp_address, SqlTable::getIp_address)
                .flatMap(new EnrichmentFunction());

        // Sink to Kafka
        KafkaSink<SyslogEnriched> kafkaSink = KafkaSink.<SyslogEnriched>builder()
                .setBootstrapServers("kafka-kafka-bootstrap:9095")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("syslog-enriched")
                        .setValueSerializationSchema(new SyslogEnrichedSerializationSchema())
                        .build()
                )
                .build();

        joinedStream.sinkTo(kafkaSink);

        env.execute("Flink Streaming Join Demo");
    }

    public static class EnrichmentFunction extends RichCoFlatMapFunction<Syslog, SqlTable, SyslogEnriched> {

        private MapState<String, SqlTable> ipTableState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, SqlTable> ipTableDescriptor = new MapStateDescriptor<>(
                    "ipTableState",
                    Types.STRING,
                    Types.POJO(SqlTable.class)
            );
            ipTableState = getRuntimeContext().getMapState(ipTableDescriptor);
        }

        @Override
        public void flatMap1(Syslog syslog, Collector<SyslogEnriched> out) throws Exception {
            String ipAddress = syslog.getIp_address();
            SqlTable sqlTableRow = ipTableState.get(ipAddress);
            if (sqlTableRow != null) {
                SyslogEnriched enrichedSyslog = new SyslogEnriched(syslog, sqlTableRow);
                out.collect(enrichedSyslog);
            }
        }

        @Override
        public void flatMap2(SqlTable sqlTable, Collector<SyslogEnriched> out) throws Exception {
            ipTableState.put(sqlTable.getIp_address(), sqlTable);
        }
    }
}
