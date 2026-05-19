package org.example.flink.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class FlinkJobConfigTest {

    @Test
    void testDefaultConfig() {
        FlinkJobConfig config = new FlinkJobConfig();
        
        assertEquals("dev", config.getEnvironment());
        assertFalse(config.isEnableCheckpointing());
        assertEquals(Duration.ofSeconds(10), config.getTumblingWindowSize());
        assertEquals(Duration.ofMinutes(1), config.getSlidingWindowSize());
        assertEquals(Duration.ofSeconds(10), config.getSlidingWindowSlide());
        assertEquals(Duration.ofSeconds(5), config.getWatermarkDelay());
        assertEquals(5.0, config.getAlertThresholdPercentage());
        assertEquals(2.0, config.getAnomalyStdDevMultiplier());
        assertEquals(3, config.getMaxRetryAttempts());
        assertTrue(config.isEnableMetrics());
    }

    @Test
    void testProdEnvironment() {
        FlinkJobConfig config = FlinkJobConfig.forEnvironment("prod");
        
        assertEquals("prod", config.getEnvironment());
        assertTrue(config.isEnableCheckpointing());
        assertTrue(config.getCheckpointStoragePath().contains("prod"));
        assertEquals(Duration.ofMinutes(5), config.getCheckpointInterval());
        assertEquals(5, config.getMaxRetryAttempts());
        assertTrue(config.isEnableMetrics());
        assertEquals("flink-prod-group", config.getConsumerGroupId());
    }

    @Test
    void testStagingEnvironment() {
        FlinkJobConfig config = FlinkJobConfig.forEnvironment("staging");
        
        assertEquals("staging", config.getEnvironment());
        assertTrue(config.isEnableCheckpointing());
        assertTrue(config.getCheckpointStoragePath().contains("staging"));
        assertEquals(Duration.ofMinutes(2), config.getCheckpointInterval());
        assertEquals(3, config.getMaxRetryAttempts());
        assertEquals("flink-staging-group", config.getConsumerGroupId());
    }

    @Test
    void testDevEnvironment() {
        FlinkJobConfig config = FlinkJobConfig.forEnvironment("dev");
        
        assertEquals("dev", config.getEnvironment());
        assertFalse(config.isEnableCheckpointing());
        assertEquals("flink-dev-group", config.getConsumerGroupId());
    }

    @Test
    void testConfigSetters() {
        FlinkJobConfig config = new FlinkJobConfig();
        
        config.setKafkaBootstrapServers("kafka:9092");
        config.setInputTopic("crypto-prices");
        config.setAlertTopic("alerts");
        config.setDeadLetterTopic("dead-letters");
        config.setSocketHost("localhost");
        config.setSocketPort(9999);
        config.setMinPriceThreshold(0.0);
        config.setMaxPriceThreshold(1000000.0);
        
        assertEquals("kafka:9092", config.getKafkaBootstrapServers());
        assertEquals("crypto-prices", config.getInputTopic());
        assertEquals("alerts", config.getAlertTopic());
        assertEquals("dead-letters", config.getDeadLetterTopic());
        assertEquals("localhost", config.getSocketHost());
        assertEquals(9999, config.getSocketPort());
        assertEquals(0.0, config.getMinPriceThreshold());
        assertEquals(1000000.0, config.getMaxPriceThreshold());
    }

    @Test
    void testToString() {
        FlinkJobConfig config = FlinkJobConfig.forEnvironment("prod");
        config.setKafkaBootstrapServers("kafka:9092");
        config.setInputTopic("crypto-prices");
        config.setAlertTopic("alerts");
        
        String str = config.toString();
        assertTrue(str.contains("prod"));
        assertTrue(str.contains("kafka:9092"));
        assertTrue(str.contains("crypto-prices"));
    }
}
