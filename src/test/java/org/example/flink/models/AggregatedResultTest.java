package org.example.flink.models;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AggregatedResultTest {

    @Test
    void testAlertLevels() {
        AggregatedResult result = new AggregatedResult();
        
        result.setAlertLevel(AggregatedResult.AlertLevel.NORMAL);
        assertFalse(result.isAlert());
        
        result.setAlertLevel(AggregatedResult.AlertLevel.WARNING);
        assertTrue(result.isAlert());
        
        result.setAlertLevel(AggregatedResult.AlertLevel.CRITICAL);
        assertTrue(result.isAlert());
        
        result.setAlertLevel(AggregatedResult.AlertLevel.ANOMALY);
        assertTrue(result.isAlert());
    }

    @Test
    void testFullAggregationResult() {
        AggregatedResult result = new AggregatedResult();
        result.setSymbol("BTC");
        result.setSource("KAFKA");
        result.setAvgPrice(55000.0);
        result.setMinPrice(54000.0);
        result.setMaxPrice(56000.0);
        result.setStdDev(500.0);
        result.setCount(100);
        result.setTotalVolume(10000.0);
        result.setWindowStart(1672531200000L);
        result.setWindowEnd(1672531210000L);
        result.setAlertLevel(AggregatedResult.AlertLevel.WARNING);
        result.setAlertReason("Price approaching threshold");

        assertEquals("BTC", result.getSymbol());
        assertEquals(55000.0, result.getAvgPrice());
        assertEquals(54000.0, result.getMinPrice());
        assertEquals(56000.0, result.getMaxPrice());
        assertEquals(500.0, result.getStdDev());
        assertEquals(100, result.getCount());
        assertEquals(10000.0, result.getTotalVolume());
        assertEquals(1672531200000L, result.getWindowStart());
        assertEquals(1672531210000L, result.getWindowEnd());
        assertEquals(AggregatedResult.AlertLevel.WARNING, result.getAlertLevel());
        assertEquals("Price approaching threshold", result.getAlertReason());
    }

    @Test
    void testToString() {
        AggregatedResult result = new AggregatedResult();
        result.setSymbol("ETH");
        result.setSource("SOCKET");
        result.setAvgPrice(2000.0);
        result.setMinPrice(1900.0);
        result.setMaxPrice(2100.0);
        result.setStdDev(50.0);
        result.setCount(50);
        result.setAlertLevel(AggregatedResult.AlertLevel.NORMAL);
        result.setAlertReason("Within normal parameters");

        String str = result.toString();
        assertTrue(str.contains("ETH"));
        assertTrue(str.contains("2000.00"));
        assertTrue(str.contains("NORMAL"));
    }
}
