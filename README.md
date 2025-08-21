# Flink FinTech Stream Processing Project

## **Overview**

This project is a real-time financial technology (FinTech) stream processing application built with Apache Flink. It processes cryptocurrency price data from multiple sources, performs aggregations, detects alert conditions, and routes alerts to a Kafka topic for further action.
 
## Features

    Multi-source ingestion: Consumes data from both Kafka and socket streams

    Real-time processing: Processes and aggregates cryptocurrency price events

    Alert detection: Identifies price events that exceed configurable thresholds

    Side output routing: Separates alert events from normal events

    Kafka integration: Reads from and writes to Kafka topics with delivery guarantees

## Architecture

The Flink streaming pipeline processes cryptocurrency price data through these stages:

    Data Ingestion: Reads from two sources simultaneously - a Kafka topic (crypto-prices) and a socket stream (port 9999)

    Parsing & Filtering: Converts raw messages into structured PriceEvent objects and filters invalid data

    Windowed Aggregation: Groups data by cryptocurrency symbol and performs aggregations over configurable time windows

    Alert Routing: Separates processed events into two streams - alerts that exceed thresholds and normal events

    Output: Sends alerts to a Kafka topic for further processing while printing normal events to console for monitoring

The architecture remains consistent regardless of window size configuration, with data flowing sequentially through these processing stages.
 
## Components
 
### **Main Job (KafkaStreamingJob)**

The main Flink job that orchestrates the streaming pipeline:

    Data Ingestion:

        Kafka source from crypto-prices topic

        Socket stream from host.docker.internal:9999

    Data Processing:

        Parsing and validation of price events

        10-second tumbling window aggregations

        Threshold-based alert detection

    Data Output:

        Alerts published to Kafka alerts topic

        Normal events printed to stdout for debugging

### Key Classes

    PriceEvent: Data model for cryptocurrency price events

    ParseFunction: Parses raw string data into PriceEvent objects

    AggregationFunction: Computes windowed aggregations

    AlertRouter: Routes events to appropriate side outputs based on thresholds

    ThresholdUtil: Utility for managing alert thresholds

### Setup and Execution

## Prerequisites


    Java 11+

    Apache Flink

    Apache Kafka

    Docker (for containerized deployment)

## Building the Project

mvn clean package

## Running the Job

flink run target/flink-tech-1.0-SNAPSHOT.jar

## Configuration

### Kafka Settings

    Bootstrap server: kafka:9092

    Input topic: crypto-prices

    Output topic: alerts

    Consumer group: flink-group

### Processing Settings

    Window size: 10 seconds

    Processing time based windows

    Delivery guarantee: AT_LEAST_ONCE

### Input Data Format

The application expects data in the format: SYMBOL,PRICE (e.g., BTC,55000)
Outputs

    Alert events: Written to Kafka alerts topic and printed with "ALERT" prefix

    Normal events: Printed with "NORMAL" prefix for debugging

### Monitoring and Debugging

The application provides real-time debugging output:

    All alert conditions are printed to stdout with "ALERT" prefix

    Normal processing results are printed with "NORMAL" prefix

    Source identification prefixes ("KAFKA:" or "SOCKET:") help trace data origin

## Extending the Project

### To customize the application:

    Modify threshold values in ThresholdUtil

    Adjust window size in TumblingProcessingTimeWindows.of()

    Add new data sources to the merged stream

    Implement additional processing logic in the appropriate functions

### Dependencies

    Apache Flink Streaming Java

    Apache Flink Connectors (Kafka)

    Custom utility functions and models