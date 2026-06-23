package org.example.flink.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CryptoPriceProducer {

    private static final Logger LOG = LoggerFactory.getLogger(CryptoPriceProducer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BINANCE_WS_URL = "wss://stream.binance.com:9443/ws";
    private static final String COINGECKO_API = "https://api.coingecko.com/api/v3/simple/price";

    private static final int RECONNECT_BASE_DELAY_MS = 1000;
    private static final int RECONNECT_MAX_DELAY_MS = 30000;
    private static final int COINGECKO_POLL_INTERVAL_SEC = 30;

    private final KafkaProducer<String, String> kafkaProducer;
    private final String kafkaTopic;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final Map<String, String> symbolToId;

    public CryptoPriceProducer(String bootstrapServers, String topic, String currenciesResource) {
        this.kafkaTopic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        this.kafkaProducer = new KafkaProducer<>(props);

        this.symbolToId = loadSymbolToIdMap(currenciesResource);

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> loadSymbolToIdMap(String resourcePath) {
        try (InputStream is = CryptoPriceProducer.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is != null) {
                return MAPPER.readValue(is, Map.class);
            }
        } catch (Exception e) {
            LOG.error("Failed to load currency mapping from {}: {}", resourcePath, e.getMessage());
        }
        return Map.ofEntries(
                Map.entry("BTC", "bitcoin"), Map.entry("ETH", "ethereum"), Map.entry("DOGE", "dogecoin"),
                Map.entry("SOL", "solana"), Map.entry("BNB", "binancecoin"), Map.entry("XRP", "ripple"),
                Map.entry("ADA", "cardano"), Map.entry("AVAX", "avalanche-2"), Map.entry("DOT", "polkadot"),
                Map.entry("TRX", "tron"), Map.entry("SHIB", "shiba-inu"), Map.entry("LTC", "litecoin"),
                Map.entry("BCH", "bitcoin-cash"), Map.entry("XLM", "stellar"), Map.entry("LINK", "chainlink"),
                Map.entry("MATIC", "matic-network")
        );
    }

    public void start() {
        // Crypto pairs tracked on Binance (USDT pairs)
        List<String> cryptoSymbols = new ArrayList<>();
        // Fiat pairs tracked via CoinGecko
        Map<String, String> fiatPairs = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : symbolToId.entrySet()) {
            // Crypto: symbol like BTC, ETH (traded on Binance)
            // Fiat: symbol like TRY, EUR (not on Binance)
            if (isCryptoSymbol(entry.getKey())) {
                cryptoSymbols.add(entry.getKey());
            } else if (isFiatSymbol(entry.getKey())) {
                fiatPairs.put(entry.getKey(), entry.getValue());
            }
        }

        LOG.info("Starting Binance WebSocket connections for {} crypto symbols", cryptoSymbols.size());
        startBinanceStreams(cryptoSymbols);

        if (!fiatPairs.isEmpty()) {
            LOG.info("Starting CoinGecko polling for {} fiat pairs", fiatPairs.size());
            scheduler.scheduleAtFixedRate(
                    () -> pollCoinGecko(fiatPairs),
                    0, COINGECKO_POLL_INTERVAL_SEC, TimeUnit.SECONDS
            );
        }
    }

    private boolean isCryptoSymbol(String symbol) {
        return Set.of("BTC", "ETH", "DOGE", "SOL", "BNB", "XRP", "ADA",
                "AVAX", "DOT", "TRX", "SHIB", "LTC", "BCH", "XLM", "LINK", "MATIC")
                .contains(symbol.toUpperCase());
    }

    private boolean isFiatSymbol(String symbol) {
        return Set.of("TRY", "USD", "EUR", "GBP", "CNY", "JPY", "AUD",
                "CAD", "CHF", "INR", "KRW", "RUB")
                .contains(symbol.toUpperCase());
    }

    private void startBinanceStreams(List<String> symbols) {
        for (String symbol : symbols) {
            String streamName = symbol.toLowerCase() + "usdt@ticker";
            connectBinanceWebSocket(streamName, symbol, 0);
        }
    }

    private void connectBinanceWebSocket(String streamName, String symbol, int attempt) {
        if (!running.get()) return;

        String url = BINANCE_WS_URL + "/" + streamName;
        HttpClient client = HttpClient.newHttpClient();

        WebSocket.Listener listener = new WebSocket.Listener() {
            private final StringBuilder messageBuffer = new StringBuilder();

            @Override
            public void onOpen(WebSocket webSocket) {
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                messageBuffer.append(data);
                if (last) {
                    String fullMessage = messageBuffer.toString();
                    messageBuffer.setLength(0);
                    handleBinanceMessage(fullMessage, symbol);
                }
                webSocket.request(1);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                LOG.error("WebSocket error for {}: {}", streamName, error.getMessage());
                webSocket.abort();
                scheduleReconnect(streamName, symbol, attempt);
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                LOG.warn("WebSocket closed for {}: {} {}", streamName, statusCode, reason);
                if (running.get()) {
                    scheduleReconnect(streamName, symbol, attempt);
                }
                return CompletableFuture.completedFuture(null);
            }
        };

        client.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(URI.create(url), listener)
                .thenAccept(ws -> LOG.info("Connected to Binance stream: {}", streamName))
                .exceptionally(e -> {
                    LOG.error("Failed to connect to {}: {}", streamName, e.getMessage());
                    scheduleReconnect(streamName, symbol, attempt);
                    return null;
                });
    }

    private void scheduleReconnect(String streamName, String symbol, int attempt) {
        if (!running.get()) return;
        int delay = Math.min(RECONNECT_BASE_DELAY_MS * (1 << attempt), RECONNECT_MAX_DELAY_MS);
        LOG.info("Reconnecting to {} in {}ms (attempt {})", streamName, delay, attempt + 1);
        scheduler.schedule(() -> connectBinanceWebSocket(streamName, symbol, attempt + 1),
                delay, TimeUnit.MILLISECONDS);
    }

    private void handleBinanceMessage(String message, String symbol) {
        try {
            JsonNode root = MAPPER.readTree(message);
            if (root.has("e") && "24hrTicker".equals(root.get("e").asText())) {
                if (root.has("c")) {
                    String priceStr = root.get("c").asText();
                    String volumeStr = root.has("v") ? root.get("v").asText() : "0";
                    long eventTime = root.has("E") ? root.get("E").asLong() : System.currentTimeMillis();

                    double price = Double.parseDouble(priceStr);
                    double volume = Double.parseDouble(volumeStr);

                    String kafkaMessage = String.format("%s,%.2f,%d,%.4f",
                            symbol, price, eventTime, volume);
                    sendToKafka(symbol, kafkaMessage);
                }
            }
        } catch (Exception e) {
            LOG.error("Error processing Binance message for {}: {}", symbol, e.getMessage());
        }
    }

    private void pollCoinGecko(Map<String, String> fiatPairs) {
        try {
            List<String> ids = new ArrayList<>(fiatPairs.values());
            if (ids.isEmpty()) return;

            for (int i = 0; i < ids.size(); i += 100) {
                List<String> batch = ids.subList(i, Math.min(i + 100, ids.size()));
                String joinedIds = String.join(",", batch);
                String urlString = COINGECKO_API + "?ids=" + joinedIds + "&vs_currencies=usd";

                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(urlString))
                        .timeout(Duration.ofSeconds(10))
                        .GET()
                        .build();

                client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body)
                        .thenAccept(body -> {
                            try {
                                JsonNode root = MAPPER.readTree(body);
                                for (Map.Entry<String, String> entry : fiatPairs.entrySet()) {
                                    String symbol = entry.getKey();
                                    String id = entry.getValue();
                                    JsonNode valueNode = root.get(id);
                                    if (valueNode != null && valueNode.has("usd")) {
                                        double usdRate = valueNode.get("usd").asDouble();
                                        String kafkaMessage = String.format("%s,%.6f,%d,0.0",
                                                symbol, usdRate, System.currentTimeMillis());
                                        sendToKafka(symbol, kafkaMessage);
                                    }
                                }
                            } catch (Exception e) {
                                LOG.error("Error parsing CoinGecko response: {}", e.getMessage());
                            }
                        })
                        .exceptionally(e -> {
                            LOG.error("CoinGecko API error: {}", e.getMessage());
                            return null;
                        });
            }
        } catch (Exception e) {
            LOG.error("Error in CoinGecko poll: {}", e.getMessage());
        }
    }

    private void sendToKafka(String symbol, String message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, symbol, message);
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Failed to send {} to Kafka: {}", symbol, exception.getMessage());
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug("Sent {} -> topic={} partition={} offset={}",
                            message, metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            LOG.error("Error sending to Kafka: {}", e.getMessage());
        }
    }

    public void shutdown() {
        if (!running.compareAndSet(true, false)) return;
        LOG.info("Shutting down CryptoPriceProducer...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        kafkaProducer.close(Duration.ofSeconds(5));
        LOG.info("CryptoPriceProducer shutdown complete");
    }

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = System.getenv().getOrDefault("KAFKA_INPUT_TOPIC", "crypto-prices");
        String currenciesResource = System.getenv().getOrDefault("CURRENCIES_RESOURCE", "tracked-currencies.json");

        LOG.info("Starting CryptoPriceProducer: kafka={}, topic={}", bootstrapServers, topic);

        CryptoPriceProducer producer = new CryptoPriceProducer(bootstrapServers, topic, currenciesResource);
        producer.start();

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            producer.shutdown();
        }
    }
}
