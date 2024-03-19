package org.apache.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PreLoading {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set up the Flink table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Preload the iptable into keyed state
        MapStateDescriptor<String, Tuple5<String, String, String, String, String>> iptableDescriptor =
                new MapStateDescriptor<>("iptable",
                        Types.STRING,
                        Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING));
        DataStream<Tuple5<String, String, String, String, String>> iptableStream = env.addSource(new IPTableSource());

        // Create a keyed state for iptable
        DataStream<Tuple5<String, String, String, String, String>> populatedIptableStream = iptableStream
                .keyBy((KeySelector<Tuple5<String, String, String, String, String>, String>) value -> value.f0)
                .map(new RichMapFunction<Tuple5<String, String, String, String, String>, Tuple5<String, String, String, String, String>>() {
                    private MapState<String, Tuple5<String, String, String, String, String>> iptableState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        iptableState = getRuntimeContext().getMapState(iptableDescriptor);
                    }

                    @Override
                    public Tuple5<String, String, String, String, String> map(Tuple5<String, String, String, String, String> value) throws Exception {
                        iptableState.put(value.f0, value);
                        return value;
                    }
                });

        // Register the populatedIptableStream as a table
        tableEnv.createTemporaryView("iptable", populatedIptableStream);

        String create_syslog_source_table = "CREATE TABLE syslog_raw (\n" +
                "    message_timestamp STRING,\n" +
                "    hostname STRING,\n" +
                "    app STRING,\n" +
                "    ID INT,\n" +
                "    ip_address STRING,\n" +
                "    description STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'syslog-generated',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap:9095',\n" +
                "    'properties.group.id' = 'syslog',\n" +
                "    'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(create_syslog_source_table);

        String create_syslog_enriched_table = "CREATE TABLE syslog_enriched (\n" +
                "    message_timestamp STRING,\n" +
                "    hostname STRING,\n" +
                "    app STRING,\n" +
                "    ip_address_k STRING,\n" +
                "    ip_address_m STRING,\n" +
                "    hostname_m STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'syslog-enriched',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap:9095',\n" +
                "    'properties.group.id' = 'syslog-enr',\n" +
                "    'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(create_syslog_enriched_table);

        // Create a new table containing the result of the join
        String insertQuery = "INSERT INTO syslog_enriched " +
                "SELECT k.message_timestamp, k.hostname, k.app, k.ip_address, m.f0, m.f1 " +
                "FROM syslog_raw AS k " +
                "JOIN iptable AS m ON k.ip_address = m.f0";

        // Execute the query
        tableEnv.executeSql(insertQuery);
    }
}
