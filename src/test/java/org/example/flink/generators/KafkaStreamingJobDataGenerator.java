package org.example.flink.generators;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.Random;

public class KafkaStreamingJobDataGenerator {
    private static final String KAFKA_TOPIC = "crypto-prices";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int SOCKET_PORT = 9999;
    private static final String[] SYMBOLS = {"BTC", "ETH", "SOL", "ADA"};

    public static void main(String[] args) {
        // Thread 1: Kafka Producer (Simulates historical/background feed)
        new Thread(KafkaStreamingJobDataGenerator::runKafkaProducer).start();

        // Thread 2: Socket Server (Simulates real-time/manual feed)
        new Thread(KafkaStreamingJobDataGenerator::runSocketServer).start();
    }

    private static void runKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Random random = new Random();
            System.out.println("🚀 Kafka Producer started...");
            while (true) {
                String symbol = SYMBOLS[random.nextInt(SYMBOLS.length)];
                double price = 1000 + (50000 * random.nextDouble());
                String message = String.format("%s,%.2f", symbol, price);

                producer.send(new ProducerRecord<>(KAFKA_TOPIC, message));
                System.out.println("[Kafka -> " + KAFKA_TOPIC + "]: " + message);
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void runSocketServer() {
        try (ServerSocket serverSocket = new ServerSocket(SOCKET_PORT)) {
            System.out.println("🚀 Socket Server listening on port " + SOCKET_PORT + "...");
            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                    System.out.println("✅ Flink connected to Socket!");
                    Random random = new Random();
                    while (true) {
                        String message = String.format("BTC,%.2f", 55000 + random.nextDouble() * 100);
                        out.println(message); // Flink's socketTextStream needs the newline
                        System.out.println("[Socket -> Port " + SOCKET_PORT + "]: " + message);
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    System.err.println("Socket connection lost: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}