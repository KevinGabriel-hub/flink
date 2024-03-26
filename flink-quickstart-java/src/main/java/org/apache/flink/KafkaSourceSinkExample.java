package org.apache.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.model.Syslog;
import org.apache.flink.schema.SyslogDeserializationSchema;
import org.apache.flink.schema.SyslogSerializationSchema;
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        KafkaSource<Syslog> source1 = KafkaSource.<Syslog>builder()
                .setBootstrapServers("kafka-1:9092")
                .setTopics("source")
                .setGroupId("group1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SyslogDeserializationSchema())
                .build();

        DataStream<Syslog> kafkaStream = env.fromSource(
                source1, WatermarkStrategy.noWatermarks(), "Kafka Source");


        KafkaSink<Syslog> sink = KafkaSink.<Syslog>builder()
                .setBootstrapServers("kafka-1:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("sink")
                        .setValueSerializationSchema(new SyslogSerializationSchema())
                        .build()
                )//.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        kafkaStream.sinkTo(sink);

        // Execute the Flink job
        env.execute("Kafka Source Sink Example");
    }
}
