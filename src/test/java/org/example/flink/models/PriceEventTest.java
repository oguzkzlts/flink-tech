package org.example.flink.models;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PriceEventTest {

    @Test
    void testBuilderPattern() {
        PriceEvent event = PriceEvent.builder()
                .symbol("BTC")
                .price(55000.0)
                .timestamp(1672531200000L)
                .source("KAFKA")
                .volume(100.5)
                .eventId("test-123")
                .build();

        assertEquals("BTC", event.getSymbol());
        assertEquals(55000.0, event.getPrice());
        assertEquals(1672531200000L, event.getTimestamp());
        assertEquals("KAFKA", event.getSource());
        assertEquals(100.5, event.getVolume());
        assertEquals("test-123", event.getEventId());
    }

    @Test
    void testCompositeKey() {
        PriceEvent event = PriceEvent.builder()
                .symbol("ETH")
                .source("SOCKET")
                .build();

        assertEquals("ETH|SOCKET", event.getCompositeKey());
    }

    @Test
    void testDefaultValidationStatus() {
        PriceEvent event = new PriceEvent();
        assertEquals(PriceEvent.ValidationStatus.VALID, event.getValidationStatus());
        assertTrue(event.isValid());
    }

    @Test
    void testIsValid() {
        PriceEvent validEvent = new PriceEvent();
        assertTrue(validEvent.isValid());

        PriceEvent enrichedEvent = PriceEvent.builder().symbol("BTC").build();
        enrichedEvent.setValidationStatus(PriceEvent.ValidationStatus.ENRICHED);
        assertTrue(enrichedEvent.isValid());

        PriceEvent invalidEvent = PriceEvent.builder().symbol("BTC").build();
        invalidEvent.setValidationStatus(PriceEvent.ValidationStatus.INVALID);
        assertFalse(invalidEvent.isValid());
    }

    @Test
    void testPriceRangeValidation() {
        PriceEvent event = PriceEvent.builder()
                .price(50000.0)
                .build();

        assertTrue(event.isPriceWithinReasonableRange(0, 100000));
        assertFalse(event.isPriceWithinReasonableRange(60000, 100000));
    }

    @Test
    void testEqualsAndHashCode() {
        PriceEvent event1 = PriceEvent.builder()
                .eventId("123")
                .symbol("BTC")
                .price(55000.0)
                .timestamp(1672531200000L)
                .source("KAFKA")
                .build();

        PriceEvent event2 = PriceEvent.builder()
                .eventId("123")
                .symbol("BTC")
                .price(55000.0)
                .timestamp(1672531200000L)
                .source("KAFKA")
                .build();

        assertEquals(event1, event2);
        assertEquals(event1.hashCode(), event2.hashCode());
    }

    @Test
    void testToString() {
        PriceEvent event = PriceEvent.builder()
                .symbol("BTC")
                .price(55000.0)
                .source("KAFKA")
                .timestamp(1672531200000L)
                .build();

        String str = event.toString();
        assertTrue(str.contains("BTC"));
        assertTrue(str.contains("55000.00"));
        assertTrue(str.contains("KAFKA"));
    }
}
