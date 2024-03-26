package org.apache.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProcessTableAPIcdc {
    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set up the Flink table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // Register the MySQL table with Flink SQL API
        String create_mysql_table = "CREATE TABLE iptable (\n" +
                "    ip_address STRING,\n" +
                "    hostname STRING,\n" +
                "    spi STRING,\n" +
                "    network STRING,\n" +
                "    region STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = 'mysql',\n" +
                "    'port' = '3306',\n" +
                "    'database-name' = 'flink',\n" +
                "    'table-name' = 'your_table_name',\n" +
                "    'username' = 'flink',\n" +
                "    'password' = 'password',\n" +
                "    'scan.incremental.snapshot.enabled' = 'true',\n" +
                "    'scan.incremental.snapshot.chunk.key-column' = 'ip_address'\n" +
                ")";
        tableEnv.executeSql(create_mysql_table);

        String create_syslog_enriched_table = "CREATE TABLE syslog_enriched (\n" +
                "    message_timestamp STRING,\n" +
                "    hostname STRING,\n" +
                "    app STRING,\n" +
                "    ip_address_k STRING,\n" +
                "    ip_address_m STRING,\n" +
                "    hostname_m STRING,\n" +
                "    PRIMARY KEY (ip_address_k) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'syslog-enriched',\n" +
                "    'properties.bootstrap.servers' = 'kafka-kafka-bootstrap:9095',\n" +
                "    'properties.group.id' = 'syslog-enr',\n" +
                "    'value.format' = 'json',\n" +
                "    'key.format' = 'json'\n" +
                ")";
        tableEnv.executeSql(create_syslog_enriched_table);

        // Create a new table containing the result of the join
        String insertQuery = "INSERT INTO syslog_enriched " +
                "SELECT k.message_timestamp, k.hostname, k.app, k.ip_address, m.ip_address, m.hostname " +
                "FROM syslog_raw AS k " +
                "JOIN iptable AS m ON k.ip_address = m.ip_address";

        // Execute the query
        tableEnv.executeSql(insertQuery);
    }
}
