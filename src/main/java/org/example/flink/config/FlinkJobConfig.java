package org.example.flink.config;

import java.io.Serializable;
import java.time.Duration;

public class FlinkJobConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String kafkaBootstrapServers;
    private String inputTopic;
    private String alertTopic;
    private String deadLetterTopic;
    private String anomalyTopic;
    private String aggregatedTopic;
    private String metricsTopic;
    private String socketHost;
    private int socketPort;
    private String consumerGroupId;
    
    private Duration tumblingWindowSize;
    private Duration slidingWindowSize;
    private Duration slidingWindowSlide;
    private Duration watermarkDelay;
    
    private boolean enableCheckpointing;
    private Duration checkpointInterval;
    private String checkpointStoragePath;
    private int checkpointRetries;
    
    private double alertThresholdPercentage;
    private double anomalyStdDevMultiplier;
    private double minPriceThreshold;
    private double maxPriceThreshold;
    
    private boolean enableMetrics;
    private Duration metricsReportingInterval;
    
    private int maxRetryAttempts;
    private Duration retryDelay;
    
    private String environment;

    public FlinkJobConfig() {
        this.tumblingWindowSize = Duration.ofSeconds(10);
        this.slidingWindowSize = Duration.ofMinutes(1);
        this.slidingWindowSlide = Duration.ofSeconds(10);
        this.watermarkDelay = Duration.ofSeconds(5);
        this.enableCheckpointing = false;
        this.checkpointInterval = Duration.ofMinutes(1);
        this.checkpointRetries = 3;
        this.alertThresholdPercentage = 5.0;
        this.anomalyStdDevMultiplier = 2.0;
        this.minPriceThreshold = 0.0;
        this.maxPriceThreshold = 1000000.0;
        this.enableMetrics = true;
        this.metricsReportingInterval = Duration.ofSeconds(30);
        this.maxRetryAttempts = 3;
        this.retryDelay = Duration.ofSeconds(1);
        this.environment = "dev";
    }

    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public void setKafkaBootstrapServers(String kafkaBootstrapServers) { this.kafkaBootstrapServers = kafkaBootstrapServers; }
    
    public String getInputTopic() { return inputTopic; }
    public void setInputTopic(String inputTopic) { this.inputTopic = inputTopic; }
    
    public String getAlertTopic() { return alertTopic; }
    public void setAlertTopic(String alertTopic) { this.alertTopic = alertTopic; }
    
    public String getDeadLetterTopic() { return deadLetterTopic; }
    public void setDeadLetterTopic(String deadLetterTopic) { this.deadLetterTopic = deadLetterTopic; }
    
    public String getAnomalyTopic() { return anomalyTopic; }
    public void setAnomalyTopic(String anomalyTopic) { this.anomalyTopic = anomalyTopic; }
    
    public String getAggregatedTopic() { return aggregatedTopic; }
    public void setAggregatedTopic(String aggregatedTopic) { this.aggregatedTopic = aggregatedTopic; }
    
    public String getMetricsTopic() { return metricsTopic; }
    public void setMetricsTopic(String metricsTopic) { this.metricsTopic = metricsTopic; }
    
    public String getSocketHost() { return socketHost; }
    public void setSocketHost(String socketHost) { this.socketHost = socketHost; }
    
    public int getSocketPort() { return socketPort; }
    public void setSocketPort(int socketPort) { this.socketPort = socketPort; }
    
    public String getConsumerGroupId() { return consumerGroupId; }
    public void setConsumerGroupId(String consumerGroupId) { this.consumerGroupId = consumerGroupId; }
    
    public Duration getTumblingWindowSize() { return tumblingWindowSize; }
    public void setTumblingWindowSize(Duration tumblingWindowSize) { this.tumblingWindowSize = tumblingWindowSize; }
    
    public Duration getSlidingWindowSize() { return slidingWindowSize; }
    public void setSlidingWindowSize(Duration slidingWindowSize) { this.slidingWindowSize = slidingWindowSize; }
    
    public Duration getSlidingWindowSlide() { return slidingWindowSlide; }
    public void setSlidingWindowSlide(Duration slidingWindowSlide) { this.slidingWindowSlide = slidingWindowSlide; }
    
    public Duration getWatermarkDelay() { return watermarkDelay; }
    public void setWatermarkDelay(Duration watermarkDelay) { this.watermarkDelay = watermarkDelay; }
    
    public boolean isEnableCheckpointing() { return enableCheckpointing; }
    public void setEnableCheckpointing(boolean enableCheckpointing) { this.enableCheckpointing = enableCheckpointing; }
    
    public Duration getCheckpointInterval() { return checkpointInterval; }
    public void setCheckpointInterval(Duration checkpointInterval) { this.checkpointInterval = checkpointInterval; }
    
    public String getCheckpointStoragePath() { return checkpointStoragePath; }
    public void setCheckpointStoragePath(String checkpointStoragePath) { this.checkpointStoragePath = checkpointStoragePath; }
    
    public int getCheckpointRetries() { return checkpointRetries; }
    public void setCheckpointRetries(int checkpointRetries) { this.checkpointRetries = checkpointRetries; }
    
    public double getAlertThresholdPercentage() { return alertThresholdPercentage; }
    public void setAlertThresholdPercentage(double alertThresholdPercentage) { this.alertThresholdPercentage = alertThresholdPercentage; }
    
    public double getAnomalyStdDevMultiplier() { return anomalyStdDevMultiplier; }
    public void setAnomalyStdDevMultiplier(double anomalyStdDevMultiplier) { this.anomalyStdDevMultiplier = anomalyStdDevMultiplier; }
    
    public double getMinPriceThreshold() { return minPriceThreshold; }
    public void setMinPriceThreshold(double minPriceThreshold) { this.minPriceThreshold = minPriceThreshold; }
    
    public double getMaxPriceThreshold() { return maxPriceThreshold; }
    public void setMaxPriceThreshold(double maxPriceThreshold) { this.maxPriceThreshold = maxPriceThreshold; }
    
    public boolean isEnableMetrics() { return enableMetrics; }
    public void setEnableMetrics(boolean enableMetrics) { this.enableMetrics = enableMetrics; }
    
    public Duration getMetricsReportingInterval() { return metricsReportingInterval; }
    public void setMetricsReportingInterval(Duration metricsReportingInterval) { this.metricsReportingInterval = metricsReportingInterval; }
    
    public int getMaxRetryAttempts() { return maxRetryAttempts; }
    public void setMaxRetryAttempts(int maxRetryAttempts) { this.maxRetryAttempts = maxRetryAttempts; }
    
    public Duration getRetryDelay() { return retryDelay; }
    public void setRetryDelay(Duration retryDelay) { this.retryDelay = retryDelay; }
    
    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }

    public static FlinkJobConfig forEnvironment(String env) {
        FlinkJobConfig config = new FlinkJobConfig();
        config.setEnvironment(env);
        
        if ("prod".equalsIgnoreCase(env)) {
            config.setEnableCheckpointing(true);
            config.setCheckpointStoragePath("s3://flink-checkpoints/prod/");
            config.setCheckpointInterval(Duration.ofMinutes(5));
            config.setMaxRetryAttempts(5);
            config.setEnableMetrics(true);
            config.setConsumerGroupId("flink-prod-group");
        } else if ("staging".equalsIgnoreCase(env)) {
            config.setEnableCheckpointing(true);
            config.setCheckpointStoragePath("s3://flink-checkpoints/staging/");
            config.setCheckpointInterval(Duration.ofMinutes(2));
            config.setMaxRetryAttempts(3);
            config.setEnableMetrics(true);
            config.setConsumerGroupId("flink-staging-group");
        } else {
            config.setEnableCheckpointing(false);
            config.setConsumerGroupId("flink-dev-group");
        }
        
        return config;
    }

    @Override
    public String toString() {
        return String.format("FlinkJobConfig{env=%s, kafka=%s, inputTopic=%s, alertTopic=%s, checkpointing=%s}",
                environment, kafkaBootstrapServers, inputTopic, alertTopic, enableCheckpointing);
    }
}
