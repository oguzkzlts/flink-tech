package org.example.flink.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class TrackedCurrencyConfig {

    private static final Map<String, String> symbolToId;

    static {
        Map<String, String> temp = Collections.emptyMap();
        try (InputStream is = TrackedCurrencyConfig.class.getClassLoader()
                .getResourceAsStream("tracked-currencies.json")) {

            ObjectMapper mapper = new ObjectMapper();
            temp = mapper.readValue(is, Map.class);

        } catch (Exception e) {
            System.err.println("Failed to load tracked currencies config: " + e.getMessage());
        }
        symbolToId = temp;
    }

    public static Map<String, String> getSymbolToIdMap() {
        return symbolToId;
    }
}
