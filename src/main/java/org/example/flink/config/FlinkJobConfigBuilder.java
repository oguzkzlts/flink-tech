package org.example.flink.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;

public class FlinkJobConfigBuilder {

    public static FlinkJobConfig buildFromArgs(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        String env = params.get("env", "dev");
        
        FlinkJobConfig config = FlinkJobConfig.forEnvironment(env);
        
        config.setKafkaBootstrapServers(params.get("kafka.brokers", "kafka:9092"));
        config.setInputTopic(params.get("input.topic", "crypto-prices"));
        config.setAlertTopic(params.get("alert.topic", "alerts"));
        config.setAnomalyTopic(params.get("anomaly.topic", "anomalies"));
        config.setDeadLetterTopic(params.get("dlq.topic", "dead-letters"));
        config.setAggregatedTopic(params.get("aggregated.topic", "aggregated-prices"));
        config.setMetricsTopic(params.get("metrics.topic", "flink-metrics"));
        config.setSocketHost(params.get("socket.host", "host.docker.internal"));
        config.setSocketPort(params.getInt("socket.port", 9999));
        config.setConsumerGroupId(params.get("consumer.group", config.getConsumerGroupId()));
        
        if (params.has("tumbling.window.seconds")) {
            config.setTumblingWindowSize(Duration.ofSeconds(params.getInt("tumbling.window.seconds")));
        }
        if (params.has("sliding.window.seconds")) {
            config.setSlidingWindowSize(Duration.ofSeconds(params.getInt("sliding.window.seconds")));
        }
        if (params.has("sliding.slide.seconds")) {
            config.setSlidingWindowSlide(Duration.ofSeconds(params.getInt("sliding.slide.seconds")));
        }
        if (params.has("watermark.delay.seconds")) {
            config.setWatermarkDelay(Duration.ofSeconds(params.getInt("watermark.delay.seconds")));
        }
        if (params.has("alert.threshold.percent")) {
            config.setAlertThresholdPercentage(params.getDouble("alert.threshold.percent"));
        }
        if (params.has("anomaly.stddev.multiplier")) {
            config.setAnomalyStdDevMultiplier(params.getDouble("anomaly.stddev.multiplier"));
        }
        if (params.has("s3.path")) {
            config.setCheckpointStoragePath(params.get("s3.path"));
        }
        if (params.has("checkpoint.interval.seconds")) {
            config.setCheckpointInterval(Duration.ofSeconds(params.getInt("checkpoint.interval.seconds")));
        }
        if (params.has("max.retry.attempts")) {
            config.setMaxRetryAttempts(params.getInt("max.retry.attempts"));
        }
        if (params.has("min.price.threshold")) {
            config.setMinPriceThreshold(params.getDouble("min.price.threshold"));
        }
        if (params.has("max.price.threshold")) {
            config.setMaxPriceThreshold(params.getDouble("max.price.threshold"));
        }
        
        return config;
    }
}
