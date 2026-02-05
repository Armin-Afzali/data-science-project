#!/usr/bin/env python3

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit, window, count, 
    current_timestamp, expr, to_timestamp, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SPARK_MASTER = os.environ.get('SPARK_MASTER', 'local[*]')
INPUT_TOPIC = 'bank_transactions'

# Categorization thresholds
MACRO_THRESHOLD = 500.0
MICRO_THRESHOLD = 20.0

# Bot detection parameters
BOT_WINDOW_DURATION = "10 seconds"
BOT_TRANSACTION_THRESHOLD = 4


def create_spark_session():
    """Create and configure Spark session with Kafka support."""
    logger.info("🔧 Creating Spark Session...")
    
    spark = SparkSession.builder \
        .appName("FraudDetectionPipeline") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✓ Spark Session created successfully")
    
    return spark


def define_transaction_schema():
    """Define the schema for incoming transaction JSON."""
    return StructType([
        StructField("Transaction_ID", StringType(), True),
        StructField("User_ID", StringType(), True),
        StructField("Timestamp", StringType(), True),
        StructField("Time", DoubleType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("Class", IntegerType(), True),
        StructField("Bot_Sequence", IntegerType(), True),
        # V1-V28 PCA components
        StructField("V1", DoubleType(), True),
        StructField("V2", DoubleType(), True),
        StructField("V3", DoubleType(), True),
        StructField("V4", DoubleType(), True),
        StructField("V5", DoubleType(), True),
        StructField("V6", DoubleType(), True),
        StructField("V7", DoubleType(), True),
        StructField("V8", DoubleType(), True),
        StructField("V9", DoubleType(), True),
        StructField("V10", DoubleType(), True),
        StructField("V11", DoubleType(), True),
        StructField("V12", DoubleType(), True),
        StructField("V13", DoubleType(), True),
        StructField("V14", DoubleType(), True),
        StructField("V15", DoubleType(), True),
        StructField("V16", DoubleType(), True),
        StructField("V17", DoubleType(), True),
        StructField("V18", DoubleType(), True),
        StructField("V19", DoubleType(), True),
        StructField("V20", DoubleType(), True),
        StructField("V21", DoubleType(), True),
        StructField("V22", DoubleType(), True),
        StructField("V23", DoubleType(), True),
        StructField("V24", DoubleType(), True),
        StructField("V25", DoubleType(), True),
        StructField("V26", DoubleType(), True),
        StructField("V27", DoubleType(), True),
        StructField("V28", DoubleType(), True),
    ])


def create_blacklist_dataframe(spark):
    """Create a manual blacklist dataframe containing blocked users."""
    logger.info("Creating blacklist dataframe...")
    
    blacklist_data = [
        ("User_Hacker", "Known Fraud Actor", "2024-01-01"),
        ("Suspicious_User_001", "Multiple Fraud Attempts", "2024-03-15"),
        ("Blocked_Account_42", "Identity Theft", "2024-06-20"),
    ]
    
    blacklist_schema = StructType([
        StructField("User_ID", StringType(), True),
        StructField("Block_Reason", StringType(), True),
        StructField("Block_Date", StringType(), True),
    ])
    
    blacklist_df = spark.createDataFrame(blacklist_data, blacklist_schema)
    logger.info(f"✓ Blacklist created with {blacklist_df.count()} users")
    
    return blacklist_df


def read_kafka_stream(spark, schema):
    """Read streaming data from Kafka topic."""
    logger.info(f"Connecting to Kafka topic: {INPUT_TOPIC}")
    
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and extract fields
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("Timestamp")))
    
    logger.info("✓ Kafka stream connected and parsing configured")
    
    return parsed_stream


def add_transaction_category(df):
    """
    Add Category column based on transaction amount:
    - Macro: Amount > 500
    - Micro: Amount < 20
    - Normal: Everything else
    """
    return df.withColumn(
        "Category",
        when(col("Amount") > MACRO_THRESHOLD, "Macro")
        .when(col("Amount") < MICRO_THRESHOLD, "Micro")
        .otherwise("Normal")
    )


def detect_blacklisted_users(transaction_df, blacklist_df):
    """
    Join transactions with blacklist and mark blocked users.
    Returns dataframe with User_Status column.
    """
    # Left join with blacklist
    joined_df = transaction_df.join(
        blacklist_df,
        on="User_ID",
        how="left"
    )
    
    # Add User_Status column
    result_df = joined_df.withColumn(
        "User_Status",
        when(col("Block_Reason").isNotNull(), "BLOCKED")
        .otherwise("ACTIVE")
    )
    
    return result_df


def process_normal_transactions(df):
    """Process and display normal (non-alert) transactions."""
    
    normal_df = df.filter(
        (col("User_Status") == "ACTIVE") & 
        (col("Class") == 0) &
        (col("User_ID") != "User_Bot")
    ).select(
        "Transaction_ID",
        "User_ID",
        "Amount",
        "Category",
        "User_Status",
        "event_time"
    )
    
    query = normal_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .queryName("normal_transactions") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    return query


def process_fraud_alerts(df):
    """Process and display fraud alerts (blocked users and detected fraud)."""
    
    fraud_df = df.filter(
        (col("User_Status") == "BLOCKED") | 
        (col("Class") == 1)
    ).select(
        "Transaction_ID",
        "User_ID",
        "Amount",
        "Category",
        "User_Status",
        "Block_Reason",
        "Class",
        "event_time"
    ).withColumn(
        "Alert_Type",
        when(col("User_Status") == "BLOCKED", "BLOCKED_USER")
        .when(col("Class") == 1, "FRAUD_DETECTED")
        .otherwise("SUSPICIOUS")
    )
    
    query = fraud_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 50) \
        .queryName("fraud_alerts") \
        .trigger(processingTime="3 seconds") \
        .start()
    
    return query


def detect_bot_attacks(df):
    """
    Detect bot attacks using windowed aggregation.
    A user with more than 4 transactions in 10 seconds is flagged as BOT_DETECTED.
    """
    
    # Create windowed aggregation
    windowed_df = df \
        .withWatermark("event_time", "20 seconds") \
        .groupBy(
            window(col("event_time"), BOT_WINDOW_DURATION, "5 seconds"),
            col("User_ID")
        ) \
        .agg(
            count("*").alias("transaction_count"),
            expr("sum(Amount)").alias("total_amount"),
            expr("first(Category)").alias("sample_category")
        )
    
    # Filter for bot-like behavior
    bot_alerts = windowed_df.filter(
        col("transaction_count") > BOT_TRANSACTION_THRESHOLD
    ).withColumn(
        "Alert_Status", lit("BOT_DETECTED")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "User_ID",
        "transaction_count",
        "total_amount",
        "Alert_Status"
    )
    
    query = bot_alerts \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 30) \
        .queryName("bot_detection") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    return query


def create_summary_statistics(df):
    """Create rolling summary statistics of the transaction stream."""
    
    summary = df \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(
            window(col("event_time"), "30 seconds", "10 seconds")
        ) \
        .agg(
            count("*").alias("total_transactions"),
            expr("sum(CASE WHEN Category = 'Macro' THEN 1 ELSE 0 END)").alias("macro_count"),
            expr("sum(CASE WHEN Category = 'Micro' THEN 1 ELSE 0 END)").alias("micro_count"),
            expr("sum(CASE WHEN Category = 'Normal' THEN 1 ELSE 0 END)").alias("normal_count"),
            expr("sum(CASE WHEN User_Status = 'BLOCKED' THEN 1 ELSE 0 END)").alias("blocked_count"),
            expr("sum(CASE WHEN Class = 1 THEN 1 ELSE 0 END)").alias("fraud_count"),
            expr("round(avg(Amount), 2)").alias("avg_amount"),
            expr("round(sum(Amount), 2)").alias("total_amount")
        )
    
    query = summary \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .queryName("summary_statistics") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query


def main():
    """Main entry point for the Spark Streaming application."""
    
    print("=" * 70)
    print("REAL-TIME FRAUD DETECTION PIPELINE")
    print("=" * 70)
    print(f"Kafka Server: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Spark Master: {SPARK_MASTER}")
    print(f"Input Topic:  {INPUT_TOPIC}")
    print(f"Bot Detection Window: {BOT_WINDOW_DURATION}")
    print(f"Bot Threshold: > {BOT_TRANSACTION_THRESHOLD} transactions")
    print("=" * 70)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Define schema
    schema = define_transaction_schema()
    
    # Create blacklist
    blacklist_df = create_blacklist_dataframe(spark)
    
    # Read Kafka stream
    raw_stream = read_kafka_stream(spark, schema)
    
    # Add transaction category
    categorized_stream = add_transaction_category(raw_stream)
    
    # Detect blacklisted users
    processed_stream = detect_blacklisted_users(categorized_stream, blacklist_df)
    
    logger.info("Starting streaming queries...")
    print("\n" + "=" * 70)
    print("STREAMING OUTPUTS")
    print("=" * 70)
    
    # Start all streaming queries
    queries = []
    
    # 1. Normal transactions
    print("\n[QUERY 1] NORMAL TRANSACTIONS")
    print("-" * 50)
    q1 = process_normal_transactions(processed_stream)
    queries.append(q1)
    
    # 2. Fraud alerts
    print("\n[QUERY 2] FRAUD ALERTS (Blocked Users & Detected Fraud)")
    print("-" * 50)
    q2 = process_fraud_alerts(processed_stream)
    queries.append(q2)
    
    # 3. Bot detection
    print("\n[QUERY 3] BOT DETECTION (Window-based Analysis)")
    print("-" * 50)
    q3 = detect_bot_attacks(processed_stream)
    queries.append(q3)
    
    # 4. Summary statistics
    print("\n[QUERY 4] SUMMARY STATISTICS")
    print("-" * 50)
    q4 = create_summary_statistics(processed_stream)
    queries.append(q4)
    
    print("\n" + "=" * 70)
    print("✓ All streaming queries started successfully!")
    print("=" * 70)
    print("\nPress Ctrl+C to stop the application...\n")
    
    # Wait for all queries
    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        for q in queries:
            q.stop()
        spark.stop()
        logger.info("Application stopped.")


if __name__ == "__main__":
    main()
