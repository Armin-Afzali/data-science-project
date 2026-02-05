#!/usr/bin/env python3

import os
import csv
import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'bank_transactions')
DATASET_PATH = os.environ.get('DATASET_PATH', '/data/creditcard.csv')

# Producer Configuration
BOT_PROBABILITY = 0.02  # 2% probability for bot simulation
BOT_REPEAT_COUNT = 5    # Number of times to repeat bot transaction
SLEEP_MIN = 0.05        # Minimum sleep between records (seconds)
SLEEP_MAX = 0.2         # Maximum sleep between records (seconds)

# List of normal user IDs for random assignment
USER_POOL = [f"User_{i:04d}" for i in range(1, 1001)]


def create_producer():
    """Create and configure Kafka producer with retries."""
    max_retries = 30
    retry_interval = 5
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_block_ms=10000
            )
            logger.info(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Waiting for Kafka... ({e})")
            time.sleep(retry_interval)
    
    raise Exception("Failed to connect to Kafka after maximum retries")


def transform_record(row, headers, user_id):
    """Transform CSV row into a structured transaction record."""
    record = {}
    
    for i, header in enumerate(headers):
        value = row[i] if i < len(row) else None
        
        # Convert numeric fields
        if header in ['Time', 'Amount'] or header.startswith('V'):
            try:
                record[header] = float(value) if value else 0.0
            except ValueError:
                record[header] = 0.0
        elif header == 'Class':
            record[header] = int(value) if value else 0
        else:
            record[header] = value
    
    # Metadata
    record['User_ID'] = user_id
    record['Timestamp'] = datetime.now().isoformat()
    record['Transaction_ID'] = f"TXN_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    
    return record


def send_transaction(producer, record):
    """Send a single transaction to Kafka."""
    try:
        future = producer.send(
            KAFKA_TOPIC,
            key=record['User_ID'],
            value=record
        )
        # Wait for send to complete
        future.get(timeout=10)
        return True
    except Exception as e:
        logger.error(f"Error sending transaction: {e}")
        return False


def stream_transactions(producer, csv_path):
    """Stream transactions from CSV to Kafka with fraud and bot simulation."""
    
    total_sent = 0
    fraud_count = 0
    bot_attacks = 0
    normal_count = 0
    
    logger.info(f"Reading dataset from: {csv_path}")
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Read header row
            
            logger.info(f"Dataset columns: {', '.join(headers[:5])}... ({len(headers)} total)")
            logger.info("Starting transaction stream...")
            
            for row_num, row in enumerate(reader, start=1):
                if len(row) < len(headers):
                    continue
                
                # Get the Class field (fraud indicator)
                try:
                    transaction_class = int(row[headers.index('Class')])
                except (ValueError, IndexError):
                    transaction_class = 0
                
                # Determine user ID based on scenario
                if transaction_class == 1:
                    # Scenario: Real fraud - assign to User_Hacker
                    user_id = "User_Hacker"
                    record = transform_record(row, headers, user_id)
                    
                    if send_transaction(producer, record):
                        fraud_count += 1
                        total_sent += 1
                        logger.info(f"FRAUD DETECTED! Transaction {record['Transaction_ID']} - "
                                  f"User: {user_id}, Amount: ${record.get('Amount', 0):.2f}")
                
                elif random.random() < BOT_PROBABILITY:
                    # Scenario: Bot attack - send same transaction 5 times
                    user_id = "User_Bot"
                    record = transform_record(row, headers, user_id)
                    
                    logger.info(f"BOT ATTACK SIMULATION - Sending {BOT_REPEAT_COUNT} rapid transactions")
                    
                    for i in range(BOT_REPEAT_COUNT):
                        # Create new transaction ID for each repeat
                        record['Transaction_ID'] = f"TXN_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
                        record['Timestamp'] = datetime.now().isoformat()
                        record['Bot_Sequence'] = i + 1
                        
                        if send_transaction(producer, record):
                            total_sent += 1
                        
                        # Very short delay between bot transactions
                        time.sleep(0.01)
                    
                    bot_attacks += 1
                    logger.info(f"Bot attack completed - User: {user_id}, "
                              f"Amount: ${record.get('Amount', 0):.2f} x {BOT_REPEAT_COUNT}")
                
                else:
                    # Normal transaction - random user
                    user_id = random.choice(USER_POOL)
                    record = transform_record(row, headers, user_id)
                    
                    if send_transaction(producer, record):
                        normal_count += 1
                        total_sent += 1
                        
                        if total_sent % 100 == 0:
                            logger.info(f"✓ Processed {total_sent} transactions "
                                      f"(Normal: {normal_count}, Fraud: {fraud_count}, "
                                      f"Bot Attacks: {bot_attacks})")
                
                # Random delay to simulate real-time stream
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                
                # Flush periodically
                if total_sent % 50 == 0:
                    producer.flush()
                    
    except FileNotFoundError:
        logger.error(f"Dataset file not found: {csv_path}")
        raise
    except Exception as e:
        logger.error(f"Error processing dataset: {e}")
        raise
    
    # Final flush
    producer.flush()
    
    logger.info("=" * 60)
    logger.info("STREAMING COMPLETED - FINAL STATISTICS")
    logger.info("=" * 60)
    logger.info(f"   Total Transactions Sent: {total_sent}")
    logger.info(f"   Normal Transactions:     {normal_count}")
    logger.info(f"   Fraud Transactions:      {fraud_count}")
    logger.info(f"   Bot Attack Events:       {bot_attacks}")
    logger.info(f"   Bot Transactions:        {bot_attacks * BOT_REPEAT_COUNT}")
    logger.info("=" * 60)
    
    return total_sent


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("BANK TRANSACTION STREAM PRODUCER")
    logger.info("=" * 60)
    logger.info(f"Kafka Server: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Bot Probability: {BOT_PROBABILITY * 100}%")
    logger.info("=" * 60)
    
    # Wait a bit for dataset to be ready
    time.sleep(2)
    
    # Create producer
    producer = create_producer()
    
    try:
        # Stream transactions
        total = stream_transactions(producer, DATASET_PATH)
        logger.info(f"✓ Successfully streamed {total} transactions")
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        raise
    finally:
        producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    main()
