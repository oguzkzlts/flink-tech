package org.example.flink.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
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

import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

@SuppressWarnings("deprecation")
public class KafkaStreamingJob {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String envType = params.get("env", "dev");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if ("prod".equalsIgnoreCase(envType)) {
            env.enableCheckpointing(60000);

            CheckpointConfig config = env.getCheckpointConfig();

            config.setCheckpointingMode(EXACTLY_ONCE);

            config.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

            // State Backend
            env.setStateBackend(new org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend(true));

            // Use a URI to resolve the 'setCheckpointStorage(String)' deprecation
            String s3Path = params.getRequired("s3.path");
            config.setCheckpointStorage(java.net.URI.create(s3Path));

            System.out.println("MODE: PRODUCTION | S3: " + s3Path);
        } else {
            System.out.println("MODE: DEVELOPMENT");
        }

        final OutputTag<String> alertOutput = new OutputTag<>("alert"){};
        final OutputTag<String> normalOutput = new OutputTag<>("normal"){};
        final OutputTag<String> deadLetterOutput = new OutputTag<>("dead-letter"){};

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
                .map(new ParseFunction(deadLetterOutput))
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        SingleOutputStreamOperator<String> aggregated = parsedStream
                .keyBy(PriceEvent::getCompositeKey)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new AggregationFunction());

        SingleOutputStreamOperator<String> routed = aggregated.process(new AlertRouter(alertOutput, normalOutput));

        if ("prod".equalsIgnoreCase(envType)) {
            KafkaSink<String> alertKafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(params.get("kafka.brokers", "kafka:9092"))
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic("alerts")
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            routed.getSideOutput(alertOutput).sinkTo(alertKafkaSink);

            KafkaSink<String> deadLetterKafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(params.get("kafka.brokers", "kafka:9092"))
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(params.get("dlq.topic", "dead-letters"))
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            parsedStream.getSideOutput(deadLetterOutput).sinkTo(deadLetterKafkaSink);
        }

        routed.getSideOutput(alertOutput).print("ALERT");
        routed.getSideOutput(normalOutput).print("NORMAL");
        parsedStream.getSideOutput(deadLetterOutput).print("DEAD-LETTER");

        env.execute("Flink-Fintech-Processor-" + envType.toUpperCase());
    }
}