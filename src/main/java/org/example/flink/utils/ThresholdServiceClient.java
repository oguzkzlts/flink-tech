package org.example.flink.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.flink.config.TrackedCurrencyConfig;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

public class ThresholdServiceClient {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Map<String, String> symbolToId = TrackedCurrencyConfig.getSymbolToIdMap();
    private static final Map<String, Double> thresholdCache = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private static final String API_TEMPLATE =
            "https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd";

    static {
        updateThresholds();
        scheduler.scheduleAtFixedRate(ThresholdServiceClient::updateThresholds, 0, 60, TimeUnit.SECONDS);
    }

    public static double getThreshold(String symbol) {
        return thresholdCache.getOrDefault(symbol.toUpperCase(), Double.MAX_VALUE);
    }

    private static void updateThresholds() {
        try {
            List<String> ids = new ArrayList<>(symbolToId.values());
            int batchSize = 100; // CoinGecko limit
            for (int i = 0; i < ids.size(); i += batchSize) {
                List<String> batch = ids.subList(i, Math.min(i + batchSize, ids.size()));
                String joinedIds = String.join(",", batch);
                String url = String.format(API_TEMPLATE, joinedIds);

                HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);

                JsonNode root = mapper.readTree(conn.getInputStream());

                for (Map.Entry<String, String> entry : symbolToId.entrySet()) {
                    String symbol = entry.getKey().toUpperCase();
                    String id = entry.getValue();
                    JsonNode valueNode = root.get(id);
                    if (valueNode != null && valueNode.has("usd")) {
                        double value = valueNode.get("usd").asDouble();
                        double threshold = value * 1.05; // for example: 5% above current value
                        thresholdCache.put(symbol, threshold);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error updating thresholds: " + e.getMessage());
        }
    }
}
