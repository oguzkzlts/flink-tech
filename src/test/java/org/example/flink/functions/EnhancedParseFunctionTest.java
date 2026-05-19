package org.example.flink.functions;

import org.example.flink.config.FlinkJobConfig;
import org.example.flink.models.PriceEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EnhancedParseFunctionTest {

    private EnhancedParseFunction parseFunction;

    @BeforeEach
    void setUp() {
        parseFunction = new EnhancedParseFunction();
    }

    @Test
    void testValidKafkaFormat() throws Exception {
        String input = "KAFKA:BTC,55000.50,1672531200000";
        PriceEvent result = parseFunction.map(input);

        assertNotNull(result);
        assertEquals("BTC", result.getSymbol());
        assertEquals(55000.50, result.getPrice());
        assertEquals(1672531200000L, result.getTimestamp());
        assertEquals("KAFKA", result.getSource());
        assertTrue(result.isValid());
    }

    @Test
    void testValidSocketFormat() throws Exception {
        String input = "SOCKET:ETH,2000.75";
        PriceEvent result = parseFunction.map(input);

        assertNotNull(result);
        assertEquals("ETH", result.getSymbol());
        assertEquals(2000.75, result.getPrice());
        assertEquals("SOCKET", result.getSource());
        assertNotNull(result.getTimestamp());
        assertTrue(result.getTimestamp() <= System.currentTimeMillis());
    }

    @Test
    void testNullInput() throws Exception {
        PriceEvent result = parseFunction.map(null);
        assertNull(result);
    }

    @Test
    void testEmptyInput() throws Exception {
        PriceEvent result = parseFunction.map("");
        assertNull(result);
    }

    @Test
    void testInvalidFormatMissingColon() throws Exception {
        PriceEvent result = parseFunction.map("BTC,55000");
        assertNull(result);
    }

    @Test
    void testInvalidFormatMissingPrice() throws Exception {
        PriceEvent result = parseFunction.map("KAFKA:BTC");
        assertNull(result);
    }

    @Test
    void testInvalidPriceFormat() throws Exception {
        PriceEvent result = parseFunction.map("KAFKA:BTC,not_a_number");
        assertNull(result);
    }

    @Test
    void testNegativePrice() throws Exception {
        PriceEvent result = parseFunction.map("KAFKA:BTC,-100");
        assertNull(result);
    }

    @Test
    void testSymbolCaseNormalization() throws Exception {
        String input = "KAFKA:btc,55000";
        PriceEvent result = parseFunction.map(input);

        assertNotNull(result);
        assertEquals("BTC", result.getSymbol());
    }

    @Test
    void testVolumeParsing() throws Exception {
        String input = "KAFKA:SOL,100.50,1672531200000,1234.56";
        PriceEvent result = parseFunction.map(input);

        assertNotNull(result);
        assertEquals(1234.56, result.getVolume());
    }

    @Test
    void testEventIdGeneration() throws Exception {
        String input = "KAFKA:BTC,55000";
        PriceEvent result1 = parseFunction.map(input);
        PriceEvent result2 = parseFunction.map(input);

        assertNotNull(result1.getEventId());
        assertNotNull(result2.getEventId());
        assertNotEquals(result1.getEventId(), result2.getEventId());
    }
}
