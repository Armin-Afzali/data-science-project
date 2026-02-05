# 🏦 Real-Time Bank Transaction Fraud Detection Pipeline

A Big Data streaming pipeline for real-time analysis of banking transactions and fraud detection using **Apache Kafka** and **Apache Spark Structured Streaming**.

## 📋 Project Overview

This project implements a complete data pipeline that:

1. **Streams transaction data** from the Credit Card Fraud Detection dataset through Kafka
2. **Processes transactions in real-time** using Spark Structured Streaming
3. **Detects various types of fraud and anomalies**:
   - Known fraud actors (User_Hacker) via blacklist
   - Suspicious transactions (Class=1 in dataset)
   - Bot attacks using windowed time-series analysis (>4 transactions in 10 seconds)
4. **Categorizes transactions** by amount (Macro/Micro/Normal)

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   creditcard    │     │     Apache      │     │  Spark Struct.  │
│     .csv        │ ──▶ │     Kafka       │ ──▶ │   Streaming     │
│   (Dataset)     │     │ (Message Queue) │     │  (Processing)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
   ┌─────────┐            ┌─────────┐           ┌──────────────┐
   │Producer │            │ Topic:  │           │   Outputs:   │
   │ Script  │            │ bank_   │           │ • Alerts     │
   │         │            │ transac │           │ • Bot Detect │
   │ • User_ │            │ tions   │           │ • Statistics │
   │   Hacker│            └─────────┘           └──────────────┘
   │ • User_ │
   │   Bot   │
   └─────────┘
```

## 🚀 Quick Start

### Prerequisites

- **Podman** & **Podman Compose** (or Docker & Docker Compose)
- 4GB+ RAM available
- ~500MB disk space for dataset

### 1. Clone and Setup

```bash
cd fraud-detection-project

# Download the dataset
mkdir -p data
curl -L -o data/creditcard.csv \
  "https://github.com/nsethi31/Kaggle-Data-Credit-Card-Fraud-Detection/raw/master/creditcard.csv"

# Copy dataset to producer folder (required for Podman)
cp data/creditcard.csv kafka-producer/
```

### 2. Download Spark Kafka JARs

The Spark container needs Kafka connector JARs:

```bash
mkdir -p spark-jars

curl -L -o spark-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

curl -L -o spark-jars/kafka-clients-3.4.1.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

curl -L -o spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

curl -L -o spark-jars/commons-pool2-2.11.1.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
```

### 3. Start the Pipeline

```bash
# Start all services
podman-compose up -d --build

# Wait for services to initialize (30 seconds)
sleep 30

# Check all services are running
podman-compose ps
```

### 4. Monitor the Pipeline

```bash
# Watch producer streaming transactions
podman-compose logs -f kafka-producer

# Watch Spark processing and fraud detection
podman-compose logs -f spark-processor
```

### 5. Stop the Pipeline

```bash
podman-compose down -v
```

## 📁 Project Structure

```
fraud-detection-project/
├── docker-compose.yml              # Podman/Docker orchestration
├── README.md                       # This file
├── kafka-producer/
│   ├── Dockerfile                  # Producer container
│   ├── producer.py                 # Kafka producer script
│   └── creditcard.csv              # Dataset (copied here for Podman)
├── spark-processor/
│   └── processor.py                # Spark streaming processor
├── spark-jars/                     # Kafka connector JARs
│   ├── spark-sql-kafka-0-10_2.12-3.5.0.jar
│   ├── kafka-clients-3.4.1.jar
│   ├── spark-token-provider-kafka-0-10_2.12-3.5.0.jar
│   └── commons-pool2-2.11.1.jar
├── data/
│   └── creditcard.csv              # Original dataset
└── docs/
    └── Project_Report.docx         # Project documentation
```

## 🔧 Components

### 1. Kafka Producer (`kafka-producer/producer.py`)

Reads the creditcard.csv dataset and streams transactions to Kafka with the following logic:

| Scenario | Condition | User Assignment | Action |
|----------|-----------|-----------------|--------|
| **Fraud** | Class=1 in CSV | `User_Hacker` | Single transaction |
| **Bot Attack** | 2% random probability | `User_Bot` | 5 rapid consecutive transactions |
| **Normal** | Default | `User_0001` - `User_1000` | Single transaction |

### 2. Spark Processor (`spark-processor/processor.py`)

Processes the stream with multiple real-time analyses:

#### Transaction Categorization
| Amount | Category |
|--------|----------|
| > $500 | **Macro** |
| < $20 | **Micro** |
| $20 - $500 | **Normal** |

#### Blacklist Detection
- Maintains a blacklist DataFrame containing `User_Hacker`
- Performs LEFT JOIN with live stream
- Marks matched users as `BLOCKED`

#### Bot Detection (Windowed Analysis)
- **Window Duration**: 10 seconds (sliding, 5-second intervals)
- **Threshold**: More than 4 transactions
- **Alert**: Users exceeding threshold flagged as `BOT_DETECTED`

## 📊 Output Streams

The processor outputs four concurrent streaming queries:

| Query | Description | Output Mode |
|-------|-------------|-------------|
| **Normal Transactions** | Active users, non-fraud transactions | Append |
| **Fraud Alerts** | Blocked users and Class=1 fraud | Append |
| **Bot Detection** | High-frequency transaction alerts | Update |
| **Summary Statistics** | Aggregated metrics per time window | Update |

### Sample Output

**Producer Log:**
```
✓ Connected to Kafka at kafka:29092
🚀 Starting transaction stream...
🔴 FRAUD DETECTED! Transaction TXN_1770283968053_6704 - User: User_Hacker, Amount: $179.66
🤖 BOT ATTACK SIMULATION - Sending 5 rapid transactions
🤖 Bot attack completed - User: User_Bot, Amount: $18.13 x 5
✓ Processed 10000 transactions (Normal: 9054, Fraud: 36, Bot Attacks: 182)
```

**Bot Detection:**
```
+-------------------+-------------------+--------+-----------------+------------+
|window_start       |window_end         |User_ID |transaction_count|Alert_Status|
+-------------------+-------------------+--------+-----------------+------------+
|2026-02-05 09:33:15|2026-02-05 09:33:25|User_Bot|5                |BOT_DETECTED|
+-------------------+-------------------+--------+-----------------+------------+
```

**Summary Statistics:**
```
+------------------+-----------+-----------+------------+-------------+-----------+----------+
|total_transactions|macro_count|micro_count|normal_count|blocked_count|fraud_count|avg_amount|
+------------------+-----------+-----------+------------+-------------+-----------+----------+
|175               |4          |80         |91          |1            |1          |68.33     |
+------------------+-----------+-----------+------------+-------------+-----------+----------+
```

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `KAFKA_TOPIC` | `bank_transactions` | Input topic name |

### Tuning Parameters

**Producer (`producer.py`):**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `BOT_PROBABILITY` | 0.02 (2%) | Chance of bot simulation |
| `BOT_REPEAT_COUNT` | 5 | Transactions per bot attack |
| `SLEEP_MIN/MAX` | 0.05-0.2s | Delay range between records |

**Processor (`processor.py`):**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `BOT_WINDOW_DURATION` | 10 seconds | Window for bot detection |
| `BOT_TRANSACTION_THRESHOLD` | 4 | Transactions to trigger alert |
| `MACRO_THRESHOLD` | $500 | Amount for Macro category |
| `MICRO_THRESHOLD` | $20 | Amount for Micro category |

## 🔍 Monitoring

### View Kafka Topics
```bash
# List topics
podman exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume raw messages
podman exec -it kafka kafka-console-consumer \
  --topic bank_transactions \
  --bootstrap-server localhost:9092 \
  --max-messages 5
```

### Spark UI
Access at **http://localhost:8080** to monitor:
- Active streaming jobs
- Processing rates
- Worker status

## 🛠️ Troubleshooting

### Common Issues

| Problem | Solution |
|---------|----------|
| Permission denied (Podman) | Use `:Z` flag on volumes, embed dataset in Docker image |
| Kafka not starting | Wait longer, check `podman-compose logs kafka` |
| Spark can't download JARs | Download JARs manually to `spark-jars/` folder |
| Producer exits immediately | Check dataset exists: `ls kafka-producer/creditcard.csv` |

### Useful Commands
```bash
# Restart a specific service
podman-compose restart spark-processor

# View all logs
podman-compose logs

# Full cleanup and restart
podman-compose down -v && podman-compose up -d --build

# Check container resource usage
podman stats
```

## 📈 Dataset

Uses the [Credit Card Fraud Detection Dataset](https://www.kaggle.com/mlg-ulb/creditcardfraud):

| Attribute | Value |
|-----------|-------|
| Total Transactions | 284,807 |
| Fraudulent | 492 (0.172%) |
| Features | 30 (Time, Amount, V1-V28, Class) |
| Format | PCA-transformed for confidentiality |

## 📚 Technologies Used

| Technology | Version | Purpose |
|------------|---------|---------|
| Apache Kafka | 7.5.0 (Confluent) | Distributed streaming platform |
| Apache Spark | 3.5.0 | Stream processing engine |
| Spark Structured Streaming | 3.5.0 | Real-time data processing |
| Python | 3.10 | Producer and processor scripts |
| Podman/Docker Compose | Latest | Container orchestration |

## 📝 License

This project is for educational purposes - Big Data Analytics Course.

---

**Course**: Streaming Data Processing  
**Project**: Real-Time Fraud Detection Pipeline  
**Date**: 2025
