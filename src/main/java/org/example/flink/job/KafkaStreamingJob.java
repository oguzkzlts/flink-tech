package org.example.flink.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode; // Use the streaming API version
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.example.flink.functions.AggregationFunction;
import org.example.flink.functions.AlertRouter;
import org.example.flink.functions.ParseFunction;
import org.example.flink.models.PriceEvent;

import java.time.Duration;
import java.util.Objects;

public class KafkaStreamingJob {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String envType = params.get("env", "dev");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- 1. ENVIRONMENT CONFIGURATION ---
        if ("prod".equalsIgnoreCase(envType)) {
            // Enable Checkpointing (1 minute)
            env.enableCheckpointing(60000);

            CheckpointConfig config = env.getCheckpointConfig();

            config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

            config.setExternalizedCheckpointCleanup(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            String s3Path = params.getRequired("s3.path");
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

            // Use the explicit storage setter
            config.setCheckpointStorage(s3Path);

            System.out.println("MODE: PRODUCTION | S3: " + s3Path);
        } else {
            System.out.println("MODE: DEVELOPMENT");
        }

        // --- 2. STREAM SETUP ---
        final OutputTag<String> alertOutput = new OutputTag<>("alert"){};
        final OutputTag<String> normalOutput = new OutputTag<>("normal"){};

        WatermarkStrategy<PriceEvent> watermarkStrategy = WatermarkStrategy
                .<PriceEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(params.get("kafka.brokers", "kafka:9092"))
                .setTopics("crypto-prices")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(line -> "KAFKA:" + line);

        DataStream<String> socketStream = env.socketTextStream(
                        params.get("socket.host", "host.docker.internal"),
                        params.getInt("socket.port", 9999))
                .map(line -> "SOCKET:" + line);

        SingleOutputStreamOperator<PriceEvent> parsedStream = kafkaStream.union(socketStream)
                .filter(line -> line.contains(","))
                .map(new ParseFunction())
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // --- 3. BUSINESS LOGIC ---
        SingleOutputStreamOperator<String> aggregated = parsedStream
                .keyBy(PriceEvent::getCompositeKey)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new AggregationFunction());

        SingleOutputStreamOperator<String> routed = aggregated.process(new AlertRouter(alertOutput, normalOutput));

        // --- 4. CONDITIONAL SINKS ---
        if ("prod".equalsIgnoreCase(envType)) {
            KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(params.get("kafka.brokers", "kafka:9092"))
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("alerts")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            routed.getSideOutput(alertOutput).sinkTo(kafkaSink);
        }

        routed.getSideOutput(alertOutput).print("ALERT");
        routed.getSideOutput(normalOutput).print("NORMAL");

        env.execute("Flink-Fintech-Processor-" + envType.toUpperCase());
    }
}