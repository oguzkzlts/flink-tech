package org.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
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
        SingleOutputStreamOperator<Tuple2<String, Double>> parsedStream = merged
                .filter(line -> line.contains(","))
                .map(line -> {
                    String[] parts = line.split(":");
                    String source = parts[0];
                    String[] values = parts[1].split(",");
                    String symbol = values[0].trim().toUpperCase();
                    double price = Double.parseDouble(values[1].trim());
                    return Tuple2.of(symbol + "|" + source, price);  // key includes source
                });

        // Windowed aggregation (10 sec) with average price per symbol
        SingleOutputStreamOperator<String> aggregated = parsedStream
                .keyBy(t -> t.f0)  // key = SYMBOL|SOURCE
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple2<String, Double>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, org.apache.flink.streaming.api.windowing.windows.TimeWindow window,
                                      Iterable<Tuple2<String, Double>> input, Collector<String> out) {
                        int count = 0;
                        double sum = 0;
                        for (Tuple2<String, Double> record : input) {
                            sum += record.f1;
                            count++;
                        }
                        double avg = sum / count;
                        String[] keyParts = key.split("\\|");
                        String symbol = keyParts[0];
                        String source = keyParts[1];
                        String result = String.format("SRC:%s | SYMBOL:%s | AVG_PRICE:%.2f | COUNT:%d", source, symbol, avg, count);

                        if (avg > getThreshold(symbol)) {
                            out.collect("ALERT: " + result);
                        } else {
                            out.collect("NORMAL: " + result);
                        }
                    }
                });

        // Side output routing
        SingleOutputStreamOperator<String> routed = aggregated.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) {
                if (value.startsWith("ALERT:")) {
                    ctx.output(alertOutput, value);
                } else {
                    ctx.output(normalOutput, value);
                }
            }
        });

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

    // Thresholds hardcoded for simplicity
    private static double getThreshold(String symbol) {
        switch (symbol) {
            case "BTC":
                return 50000.0;
            case "ETH":
                return 2000.0;
            case "DOGE":
                return 0.1;
            default:
                return Double.MAX_VALUE;
        }
    }

}
