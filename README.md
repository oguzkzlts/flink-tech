# FlinkTech - Production-Grade Real-Time Crypto Price Processing

A production-ready Apache Flink application for real-time cryptocurrency price stream processing with advanced analytics, anomaly detection, and comprehensive monitoring.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           DATA INGESTION LAYER                                   │
│                                                                                    │
│  ┌──────────────────────┐              ┌──────────────────────┐                  │
│  │   Kafka Source        │              │   Socket Source       │                  │
│  │   (crypto-prices)     │              │   (port 9999)         │                  │
│  └──────────┬───────────┘              └──────────┬───────────┘                  │
│             └──────────────────┬──────────────────┘                               │
└────────────────────────────────┼──────────────────────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         PROCESSING LAYER                                         │
│                                                                                    │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │  EnhancedParseFunction                                                    │   │
│  │  - Schema validation                                                      │   │
│  │  - Data quality checks                                                    │   │
│  │  - Event ID generation                                                    │   │
│  │  - Metrics collection                                                     │   │
│  └──────────────────────────────────┬───────────────────────────────────────┘   │
│                                     ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │  Watermark Strategy (5s out-of-order)                                     │   │
│  └──────────────────────────────────┬───────────────────────────────────────┘   │
│                                     ▼                                            │
│  ┌──────────────────────────────┐  ┌──────────────────────────────────────┐   │
│  │  Tumbling Window (10s)        │  │  Sliding Window (1m / 10s slide)    │   │
│  │  - Avg, Min, Max, StdDev     │  │  - Moving averages                    │   │
│  │  - Volume aggregation         │  │  - Trend detection                   │   │
│  │  - Anomaly detection          │  │  - Volatility analysis               │   │
│  └──────────────┬───────────────┘  └──────────────┬───────────────────────┘   │
│                 └──────────────────┬───────────────┘                           │
└────────────────────────────────────┼───────────────────────────────────────────┘
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          ROUTING LAYER                                           │
│                                                                                    │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │  EnhancedAlertRouter                                                      │   │
│  │  - CRITICAL: Price exceeds threshold                                      │   │
│  │  - WARNING: Price approaching threshold                                   │   │
│  │  - ANOMALY: High volatility detected                                      │   │
│  │  - NORMAL: Within parameters                                              │   │
│  └──────────┬──────────────┬──────────────┬────────────────────────────────┘   │
│             ▼              ▼              ▼                                     │
└─────────────────────────────────────────────────────────────────────────────────┘
             │              │              │
             ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            SINK LAYER                                            │
│                                                                                    │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ Alert Topic   │    │ Anomaly Topic│    │ Console Output│   │ Dead Letters │  │
│  │ (Kafka)       │    │ (Kafka)      │    │ (All levels)  │   │ (Kafka)      │  │
│  └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                        MONITORING STACK                                          │
│                                                                                    │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                       │
│  │ Prometheus    │◄───│ Flink Metrics│    │ Grafana      │                       │
│  │ (Metrics DB)  │    │ (JMX)        │    │ (Dashboards) │                       │
│  └──────────────┘    └──────────────┘    └──────────────┘                       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Features

### Core Processing
- **Multi-source ingestion**: Kafka + Socket streams with union
- **Event-time processing**: Watermarks with configurable out-of-order tolerance
- **Multi-window aggregations**: Tumbling and sliding windows
- **Statistical analysis**: Mean, min, max, standard deviation, volume
- **Anomaly detection**: Statistical methods using standard deviation thresholds
- **Dynamic thresholds**: Live price thresholds from CoinGecko API

### Production Features
- **Dead Letter Queue**: Failed events routed to DLQ topic for reprocessing
- **Checkpointing**: RocksDB state backend with S3 storage
- **Exactly-once semantics**: For state and Kafka output
- **Metrics collection**: Custom Flink metrics for monitoring
- **Structured logging**: SLF4J with Log4j2 implementation
- **Configuration management**: Environment-specific configs (dev/staging/prod)
- **Graceful shutdown**: Proper resource cleanup

### Monitoring & Observability
- **Prometheus**: Metrics collection and storage
- **Grafana**: Pre-configured dashboards
- **JMX exposure**: Flink metrics via JMX
- **Health checks**: Docker health checks for all services

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Java 11+ (for local development)
- Maven 3.6+

### Running with Docker Compose

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f flink-job

# Access interfaces
# Flink Dashboard: http://localhost:8081
# Grafana: http://localhost:3000 (admin/admin)
# Prometheus: http://localhost:9090
```

### Running Locally

```bash
# Build the project
mvn clean package -DskipTests

# Run in development mode
java -jar target/FlinkTech-1.0-SNAPSHOT.jar --env dev

# Run in production mode
java -jar target/FlinkTech-1.0-SNAPSHOT.jar \
  --env prod \
  --kafka.brokers localhost:9092 \
  --s3.path s3://your-bucket/flink/checkpoints/
```

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=EnhancedParseFunctionTest
```

## Configuration

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--env` | Environment (dev/staging/prod) | dev |
| `--kafka.brokers` | Kafka bootstrap servers | kafka:9092 |
| `--input.topic` | Input topic name | crypto-prices |
| `--alert.topic` | Alert topic name | alerts |
| `--dlq.topic` | Dead letter topic name | dead-letters |
| `--socket.host` | Socket source host | host.docker.internal |
| `--socket.port` | Socket source port | 9999 |
| `--tumbling.window.seconds` | Tumbling window size | 10 |
| `--sliding.window.seconds` | Sliding window size | 60 |
| `--sliding.slide.seconds` | Sliding window slide | 10 |
| `--watermark.delay.seconds` | Watermark delay | 5 |
| `--alert.threshold.percent` | Alert threshold % | 5.0 |
| `--anomaly.stddev.multiplier` | Anomaly stddev multiplier | 2.0 |
| `--s3.path` | S3 checkpoint storage path | - |
| `--checkpoint.interval.seconds` | Checkpoint interval | 60 |
| `--max.retry.attempts` | Max retry attempts | 3 |

### Environment Profiles

#### Development
- No checkpointing
- Console output only
- Single parallelism

#### Staging
- Checkpointing enabled (2min interval)
- S3 checkpoint storage
- Kafka sinks enabled

#### Production
- Checkpointing enabled (5min interval)
- S3 checkpoint storage with retention
- Kafka sinks with exactly-once
- Max retry attempts: 5

## Kafka Topics

| Topic | Partitions | Description |
|-------|-----------|-------------|
| `crypto-prices` | 3 | Input price events |
| `alerts` | 3 | Critical and warning alerts |
| `anomalies` | 3 | Statistical anomalies |
| `dead-letters` | 1 | Failed events |
| `aggregated-prices` | 3 | Aggregated results |

## Data Formats

### Input Format
```
SOURCE:SYMBOL,PRICE[,TIMESTAMP[,VOLUME]]
```

Examples:
```
KAFKA:BTC,65000.50,1672531200000,10000.0
SOCKET:ETH,3500.25
```

### Aggregated Output
```json
{
  "symbol": "BTC",
  "source": "KAFKA",
  "avgPrice": 65100.50,
  "minPrice": 64900.00,
  "maxPrice": 65300.00,
  "stdDev": 150.25,
  "count": 100,
  "totalVolume": 1000000.0,
  "windowStart": 1672531200000,
  "windowEnd": 1672531210000,
  "alertLevel": "NORMAL",
  "alertReason": "Within normal parameters"
}
```

## Metrics

### Custom Metrics
- `parse.success`: Successfully parsed events
- `parse.failure`: Failed parse attempts
- `windows.processed`: Processed windows
- `alerts.generated`: Generated alerts
- `anomalies.detected`: Detected anomalies
- `dead.letters.created`: Dead letter events
- `price.distribution`: Price histogram
- `events.per.second`: Event throughput

## Monitoring

### Grafana Dashboards
Access at http://localhost:3000

Default credentials: admin/admin

Pre-configured dashboards:
- Flink Job Overview
- Kafka Metrics
- Custom Application Metrics

### Prometheus Queries
```promql
# Event throughput
rate(flink_taskmanager_job_task_operator_events_per_second[1m])

# Alert rate
rate(flink_taskmanager_job_task_operator_alerts_generated_total[5m])

# Parse success rate
rate(flink_taskmanager_job_task_operator_parse_success_total[1m]) / 
(rate(flink_taskmanager_job_task_operator_parse_success_total[1m]) + 
 rate(flink_taskmanager_job_task_operator_parse_failure_total[1m]))
```

## Development

### Project Structure
```
src/
├── main/
│   ├── java/org/example/flink/
│   │   ├── config/           # Configuration classes
│   │   ├── functions/        # Flink functions (parse, aggregate, route)
│   │   ├── job/              # Main job orchestration
│   │   ├── models/           # Data models
│   │   └── utils/            # Utility classes
│   ├── avro/                 # Avro schemas
│   └── resources/            # Configuration files
└── test/
    └── java/org/example/flink/
        ├── config/           # Config tests
        ├── functions/        # Function tests
        ├── models/           # Model tests
        └── generators/       # Test data generators
```

### Adding New Features

1. **New aggregation metric**: Update `EnhancedAggregationFunction`
2. **New alert type**: Add to `AggregatedResult.AlertLevel` enum
3. **New data source**: Add to `ProductionKafkaStreamingJob`
4. **New sink**: Add to `configureSinks` method

## Production Deployment

### AWS MSK + S3

```bash
flink run \
  -m yarn-cluster \
  -c org.example.flink.job.ProductionKafkaStreamingJob \
  target/FlinkTech-1.0-SNAPSHOT.jar \
  --env prod \
  --kafka.brokers your-msk-endpoint:9092 \
  --s3.path s3://your-bucket/flink/checkpoints/
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-job
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink-job
  template:
    spec:
      containers:
      - name: flink-job
        image: your-registry/flink-tech:latest
        command: ["/opt/flink/bin/flink", "run"]
        args:
          - "-m"
          - "jobmanager:8081"
          - "-c"
          - "org.example.flink.job.ProductionKafkaStreamingJob"
          - "/opt/flink/usrlib/FlinkTech-1.0-SNAPSHOT.jar"
          - "--env"
          - "prod"
```

## Author

Oguzhan Kizltas

## License

MIT
