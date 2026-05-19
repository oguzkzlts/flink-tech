package org.example.flink.generators;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProductionDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(ProductionDataGenerator.class);
    
    private static final String KAFKA_TOPIC = "crypto-prices";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int SOCKET_PORT = 9999;
    
    private static final String[] SYMBOLS = {"BTC", "ETH", "SOL", "ADA", "DOGE", "BNB", "XRP", "AVAX", "DOT", "MATIC"};
    private static final Map<String, Double> BASE_PRICES = new HashMap<>();
    
    static {
        BASE_PRICES.put("BTC", 65000.0);
        BASE_PRICES.put("ETH", 3500.0);
        BASE_PRICES.put("SOL", 150.0);
        BASE_PRICES.put("ADA", 0.55);
        BASE_PRICES.put("DOGE", 0.15);
        BASE_PRICES.put("BNB", 600.0);
        BASE_PRICES.put("XRP", 0.60);
        BASE_PRICES.put("AVAX", 40.0);
        BASE_PRICES.put("DOT", 7.5);
        BASE_PRICES.put("MATIC", 0.85);
    }

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ExecutorService executor = Executors.newFixedThreadPool(2);
    private final Random random = new Random();
    private final Map<String, Double> currentPrices = new HashMap<>(BASE_PRICES);

    public static void main(String[] args) {
        ProductionDataGenerator generator = new ProductionDataGenerator();
        generator.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down data generator...");
            generator.stop();
        }));
    }

    public void start() {
        LOG.info("Starting Production Data Generator...");
        executor.submit(this::runKafkaProducer);
        executor.submit(this::runSocketServer);
    }

    public void stop() {
        running.set(false);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void runKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            LOG.info("Kafka Producer started on topic: {}", KAFKA_TOPIC);
            
            while (running.get()) {
                String symbol = SYMBOLS[random.nextInt(SYMBOLS.length)];
                double price = generateRealisticPrice(symbol);
                double volume = generateVolume(symbol);
                long timestamp = System.currentTimeMillis();
                
                String message = String.format("%s,%.2f,%d,%.2f", symbol, price, timestamp, volume);
                
                producer.send(new ProducerRecord<>(KAFKA_TOPIC, symbol, message), (metadata, exception) -> {
                    if (exception != null) {
                        LOG.error("Failed to send message: {}", exception.getMessage());
                    } else {
                        LOG.debug("Sent to Kafka: topic={}, partition={}, offset={}", 
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
                
                Thread.sleep(500 + random.nextInt(1500));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Kafka Producer interrupted");
        } catch (Exception e) {
            LOG.error("Kafka Producer error: {}", e.getMessage(), e);
        }
    }

    private void runSocketServer() {
        try (ServerSocket serverSocket = new ServerSocket(SOCKET_PORT)) {
            LOG.info("Socket Server listening on port {}...", SOCKET_PORT);
            
            while (running.get()) {
                try (Socket clientSocket = serverSocket.accept();
                     PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
                    
                    LOG.info("Client connected to socket server");
                    
                    while (running.get()) {
                        String symbol = SYMBOLS[random.nextInt(SYMBOLS.length)];
                        double price = generateRealisticPrice(symbol);
                        double volume = generateVolume(symbol);
                        long timestamp = System.currentTimeMillis();
                        
                        String message = String.format("%s,%.2f,%d,%.2f", symbol, price, timestamp, volume);
                        out.println(message);
                        
                        try {
                            Thread.sleep(1000 + random.nextInt(2000));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                } catch (IOException e) {
                    if (running.get()) {
                        LOG.error("Socket connection error: {}", e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Socket Server error: {}", e.getMessage(), e);
        }
    }

    private double generateRealisticPrice(String symbol) {
        double basePrice = currentPrices.getOrDefault(symbol, BASE_PRICES.get(symbol));
        double volatility = getVolatilityForSymbol(symbol);
        double change = (random.nextGaussian() * volatility * basePrice);
        double newPrice = basePrice + change;
        
        currentPrices.put(symbol, newPrice);
        
        if (random.nextDouble() < 0.02) {
            return newPrice * (1.5 + random.nextDouble());
        }
        
        return Math.max(0.001, newPrice);
    }

    private double getVolatilityForSymbol(String symbol) {
        switch (symbol) {
            case "BTC": return 0.002;
            case "ETH": return 0.003;
            case "SOL": return 0.005;
            case "DOGE": return 0.008;
            default: return 0.004;
        }
    }

    private double generateVolume(String symbol) {
        double baseVolume = 1000.0;
        switch (symbol) {
            case "BTC": baseVolume = 10000.0; break;
            case "ETH": baseVolume = 5000.0; break;
            case "SOL": baseVolume = 2000.0; break;
            case "DOGE": baseVolume = 50000.0; break;
        }
        return baseVolume * (0.5 + random.nextDouble() * 1.5);
    }
}
