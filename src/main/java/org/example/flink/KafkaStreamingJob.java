package org.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;

public class KafkaStreamingJob {
    public static void main(String[] args) throws Exception {
        // Set up the environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Source Configuration
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("crypto-prices")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create a stream from Kafka Source
        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Apply transformations to the stream
        DataStream<String> transformedStream = stream
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String msg) throws Exception {
                        // Add transformation logic, e.g., parsing, enriching, or filtering
                        // For example, append a timestamp to the message
                        long timestamp = System.currentTimeMillis();
                        return msg + " | Timestamp: " + timestamp;
                    }
                })
                .filter(msg -> msg.contains("test")) // Filter out messages that do not contain "Bitcoin"
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String msg) throws Exception {
                        // Further process the filtered data (e.g., transform to uppercase)
                        return msg.toUpperCase();
                    }
                });

        // Print the transformed stream for debugging
        transformedStream.print();

        // Kafka Sink Configuration
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Sink the transformed stream to Kafka
        transformedStream.sinkTo(sink);

        // Execute the Flink job
        env.execute("Kafka Processing Job");
    }
}