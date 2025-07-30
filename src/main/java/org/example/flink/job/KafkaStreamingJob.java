package org.example.flink.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.flink.functions.AggregationFunction;
import org.example.flink.functions.AlertRouter;
import org.example.flink.functions.ParseFunction;
import org.example.flink.models.PriceEvent;

import java.util.Objects;

import static org.example.flink.utils.ThresholdUtil.getThreshold;


public class KafkaStreamingJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define side outputs
        final OutputTag<String> alertOutput = new OutputTag<>("alert"){};
        final OutputTag<String> normalOutput = new OutputTag<>("normal"){};

        // Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("crypto-prices")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        ).map(line -> "KAFKA:" + line);

        // Socket stream
        DataStream<String> socketStream = env
                .socketTextStream("localhost", 9999)
                .map(line -> "SOCKET:" + line);

        // Combine streams
        DataStream<String> merged = kafkaStream.union(socketStream);

        // Parse and filter valid records (e.g., "BTC,55000")
        SingleOutputStreamOperator<PriceEvent> parsedStream = merged
                .filter(line -> line.contains(","))
                .map(new ParseFunction())
                .filter(Objects::nonNull);


        // Windowed aggregation (10 sec) with average price per symbol
        SingleOutputStreamOperator<String> aggregated = parsedStream
                .keyBy(PriceEvent::getCompositeKey)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new AggregationFunction());


        // Side output routing
        SingleOutputStreamOperator<String> routed = aggregated.process(new AlertRouter(alertOutput, normalOutput));


        // Kafka Sink (alerts only)
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Sink alerts only
        routed.getSideOutput(alertOutput).sinkTo(kafkaSink);

        // Print for debugging
        routed.getSideOutput(alertOutput).print("ALERT");
        routed.getSideOutput(normalOutput).print("NORMAL");

        env.execute("String-Based Flink Kafka Streaming Job");
    }
}
