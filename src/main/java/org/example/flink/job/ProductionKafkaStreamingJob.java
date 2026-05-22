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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.example.flink.config.FlinkJobConfig;
import org.example.flink.config.FlinkJobConfigBuilder;
import org.example.flink.functions.EnhancedAggregationFunction;
import org.example.flink.functions.EnhancedAlertRouter;
import org.example.flink.functions.EnhancedParseFunction;
import org.example.flink.models.AggregatedResult;
import org.example.flink.models.PriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProductionKafkaStreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(ProductionKafkaStreamingJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting FlinkTech Production Job...");
        
        FlinkJobConfig config = FlinkJobConfigBuilder.buildFromArgs(args);
        LOG.info("Configuration loaded: {}", config);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        configureEnvironment(env, config);

        OutputTag<AggregatedResult> alertOutput = new OutputTag<>("alert-output"){};
        OutputTag<AggregatedResult> normalOutput = new OutputTag<>("normal-output"){};
        OutputTag<AggregatedResult> anomalyOutput = new OutputTag<>("anomaly-output"){};

        WatermarkStrategy<PriceEvent> watermarkStrategy = WatermarkStrategy
                .<PriceEvent>forBoundedOutOfOrderness(config.getWatermarkDelay())
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getInputTopic())
                .setGroupId(config.getConsumerGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("enable.auto.commit", "false")
                .build();

        DataStream<String> mainStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(line -> "KAFKA:" + line);

        boolean enableSocket = "true".equalsIgnoreCase(System.getenv("ENABLE_SOCKET_SOURCE"));
        if (enableSocket) {
            LOG.info("Socket source enabled: {}:{}", config.getSocketHost(), config.getSocketPort());
            DataStream<String> socketStream = env.socketTextStream(
                            config.getSocketHost(),
                            config.getSocketPort())
                    .map(line -> "SOCKET:" + line);
            mainStream = mainStream.union(socketStream);
        } else {
            LOG.info("Socket source disabled. Using Kafka only. Set ENABLE_SOCKET_SOURCE=true to enable.");
        }

        SingleOutputStreamOperator<PriceEvent> parsedStream = mainStream
                .filter(line -> line != null && line.contains(","))
                .map(new EnhancedParseFunction())
                .filter(event -> event != null && event.isValid())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        SingleOutputStreamOperator<AggregatedResult> tumblingAggregated = parsedStream
                .keyBy(PriceEvent::getCompositeKey)
                .window(TumblingEventTimeWindows.of(config.getTumblingWindowSize()))
                .apply(new EnhancedAggregationFunction());

        SingleOutputStreamOperator<AggregatedResult> slidingAggregated = parsedStream
                .keyBy(PriceEvent::getCompositeKey)
                .window(SlidingEventTimeWindows.of(config.getSlidingWindowSize(), config.getSlidingWindowSlide()))
                .apply(new EnhancedAggregationFunction());

        DataStream<AggregatedResult> combinedStream = tumblingAggregated.union(slidingAggregated);

        SingleOutputStreamOperator<AggregatedResult> routed = combinedStream
                .process(new EnhancedAlertRouter(alertOutput, normalOutput, anomalyOutput));

        configureSinks(config, routed, alertOutput, normalOutput, anomalyOutput);

        LOG.info("Executing Flink job: FlinkTech-Production-{}", config.getEnvironment().toUpperCase());
        env.execute("FlinkTech-Production-" + config.getEnvironment().toUpperCase());
    }

    @SuppressWarnings("deprecation")
    private static void configureEnvironment(StreamExecutionEnvironment env, FlinkJobConfig config) {
        if (config.isEnableCheckpointing()) {
            env.enableCheckpointing(config.getCheckpointInterval().toMillis());

            var checkpointConfig = env.getCheckpointConfig();
            checkpointConfig.setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
            checkpointConfig.setExternalizedCheckpointCleanup(
                    org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            checkpointConfig.setMinPauseBetweenCheckpoints(1000);
            checkpointConfig.setMaxConcurrentCheckpoints(1);
            checkpointConfig.setCheckpointTimeout(60000);
            checkpointConfig.setTolerableCheckpointFailureNumber(config.getCheckpointRetries());

            env.setStateBackend(new org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend(true));
            
            if (config.getCheckpointStoragePath() != null && !config.getCheckpointStoragePath().isEmpty()) {
                checkpointConfig.setCheckpointStorage(java.net.URI.create(config.getCheckpointStoragePath()));
            }

            LOG.info("Checkpointing enabled: interval={}ms, storage={}", 
                    config.getCheckpointInterval().toMillis(), config.getCheckpointStoragePath());
        } else {
            LOG.info("Running in development mode without checkpointing");
        }

        // Initialize Prometheus metrics if enabled
        if (config.isEnableMetrics()) {
            try {
                org.apache.flink.metrics.prometheus.PrometheusMetricReporter prometheusReporter = 
                        new org.apache.flink.metrics.prometheus.PrometheusMetricReporter();
                org.apache.flink.configuration.Configuration prometheusConfig = new org.apache.flink.configuration.Configuration();
                prometheusConfig.setString("port", "9249"); // Default Prometheus port
                prometheusReporter.open(prometheusConfig);
                LOG.info("Prometheus metrics reporter initialized on port 9249");
            } catch (Exception e) {
                LOG.warn("Failed to initialize Prometheus metrics reporter: {}", e.getMessage(), e);
            }
        }

        env.setParallelism(1);
    }

    private static void configureSinks(FlinkJobConfig config,
                                      SingleOutputStreamOperator<AggregatedResult> mainStream,
                                      OutputTag<AggregatedResult> alertOutput,
                                      OutputTag<AggregatedResult> normalOutput,
                                      OutputTag<AggregatedResult> anomalyOutput) {
        
        if ("prod".equalsIgnoreCase(config.getEnvironment()) || "staging".equalsIgnoreCase(config.getEnvironment())) {
            KafkaSink<String> alertKafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(config.getKafkaBootstrapServers())
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(config.getAlertTopic())
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();

            mainStream.getSideOutput(alertOutput)
                    .map(AggregatedResult::toString)
                    .sinkTo(alertKafkaSink);

            KafkaSink<String> anomalyKafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(config.getKafkaBootstrapServers())
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(config.getAnomalyTopic())
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();

            mainStream.getSideOutput(anomalyOutput)
                    .map(AggregatedResult::toString)
                    .sinkTo(anomalyKafkaSink);

            LOG.info("Production sinks configured: alerts -> {}, anomalies -> {}", config.getAlertTopic(), config.getAnomalyTopic());
        }

        mainStream.getSideOutput(alertOutput)
                .map(AggregatedResult::toString)
                .print("ALERT");

        mainStream.getSideOutput(normalOutput)
                .map(AggregatedResult::toString)
                .print("NORMAL");

        mainStream.getSideOutput(anomalyOutput)
                .map(AggregatedResult::toString)
                .print("ANOMALY");
    }
}
