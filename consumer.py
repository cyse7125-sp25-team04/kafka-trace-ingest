from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, KafkaError, KafkaException
import os
import json
import logging
import time
import sys

# Set up logging for better debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Environment variables with validation
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092")
TOPIC = os.getenv("TOPIC", "trace_metadata")
GROUP_ID = os.getenv("GROUP_ID", "pinecone-consumer-group")

# Validate environment variables
for var_name, var_value in [("BOOTSTRAP_SERVERS", BOOTSTRAP_SERVERS), ("TOPIC", TOPIC), ("GROUP_ID", GROUP_ID)]:
    if not var_value:
        logger.error(f"Environment variable {var_name} is not set")
        sys.exit(1)

# Kafka consumer configuration with tuned settings
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Manual offset commits
    'session.timeout.ms': 30000,  # Increased to handle network latency
    'heartbeat.interval.ms': 10000,  # Frequent heartbeats
    'max.poll.interval.ms': 600000,  # Allow long processing times
    'fetch.min.bytes': 1,  # Fetch messages immediately
    'max.partition.fetch.bytes': 1048576,  # 1MB per partition
    'auto.commit.interval.ms': 5000,  # Not used due to manual commits, but set as fallback
}

# Initialize consumer with retry logic
def create_consumer():
    retries = 5
    attempt = 1
    while attempt <= retries:
        try:
            consumer = Consumer(conf)
            logger.info(f"Connected to Kafka broker at {BOOTSTRAP_SERVERS}")
            return consumer
        except KafkaException as e:
            logger.error(f"Failed to connect to Kafka (attempt {attempt}/{retries}): {e}")
            if attempt == retries:
                logger.error("Max retries reached. Exiting.")
                sys.exit(1)
            time.sleep(2 ** attempt)  # Exponential backoff
            attempt += 1

consumer = create_consumer()
consumer.subscribe([TOPIC])

# Thread pool for processing messages
executor = ThreadPoolExecutor(max_workers=5)

# Rebalance callback to log partition assignments
def rebalance_cb(consumer, partitions):
    logger.info(f"Rebalance occurred. Assigned partitions: {[p.partition for p in partitions]}")

consumer.subscribe([TOPIC], on_assign=rebalance_cb)

# Process message and handle errors
def push_to_pinecone(message):
    try:
        # Decode and process message
        decoded_message = message.decode('utf-8')
        logger.info(f"Processing message: {decoded_message}")
        # Simulate Pinecone processing (replace with actual Pinecone logic)
        time.sleep(0.1)  # Placeholder for processing time
        return True
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return False

# Main consumer loop
def consume_messages():
    try:
        while True:
            try:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition")
                    else:
                        logger.error(f"Non-recoverable error: {msg.error()}")
                    continue

                # Submit message for processing
                future = executor.submit(push_to_pinecone, msg.value())
                # Wait for processing to complete before committing
                if future.result(timeout=10):  # Timeout for processing
                    try:
                        consumer.commit(msg)
                        logger.debug(f"Committed offset for message at partition {msg.partition()} offset {msg.offset()}")
                    except KafkaException as e:
                        logger.error(f"Failed to commit offset: {e}")
                else:
                    logger.warning("Message processing failed, not committing offset")

            except KafkaException as e:
                logger.error(f"Kafka error during polling: {e}")
                time.sleep(1)  # Brief pause before retrying

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        logger.info("Closing consumer and executor...")
        executor.shutdown(wait=True)
        consumer.close()
        logger.info("Consumer closed successfully")

if __name__ == "__main__":
    consume_messages()