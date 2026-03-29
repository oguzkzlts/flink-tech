package org.example.flink.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Checkpointing & State Backend (The "Full-Fledged" config)
        env.enableCheckpointing(10000); // 10s interval
        env.getCheckpointConfig().setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);

        final OutputTag<String> alertOutput = new OutputTag<>("alert"){};
        final OutputTag<String> normalOutput = new OutputTag<>("normal"){};

        // 2. Watermark Strategy (Bounded Out-of-Orderness)
        WatermarkStrategy<PriceEvent> watermarkStrategy = WatermarkStrategy
                .<PriceEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("crypto-prices")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(line -> "KAFKA:" + line);

        DataStream<String> socketStream = env.socketTextStream("host.docker.internal", 9999)
                .map(line -> "SOCKET:" + line);

        SingleOutputStreamOperator<PriceEvent> parsedStream = kafkaStream.union(socketStream)
                .filter(line -> line.contains(","))
                .map(new ParseFunction())
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 3. Event-Time Windowing with Duration
        SingleOutputStreamOperator<String> aggregated = parsedStream
                .keyBy(PriceEvent::getCompositeKey)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new AggregationFunction());

        SingleOutputStreamOperator<String> routed = aggregated.process(new AlertRouter(alertOutput, normalOutput));

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        routed.getSideOutput(alertOutput).sinkTo(kafkaSink);
        routed.getSideOutput(alertOutput).print("ALERT");
        routed.getSideOutput(normalOutput).print("NORMAL");

        env.execute("Production-Ready Flink Stream Processor");
    }
}