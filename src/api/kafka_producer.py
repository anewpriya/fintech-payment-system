"""
Kafka producer for publishing transactions to the event stream.

This module handles all communication with Kafka, ensuring messages
are published reliably and with proper partitioning.
"""

from kafka import KafkaProducer
import json
import logging
from typing import Dict, Any, Optional

from src.config import settings

logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    Wrapper around Kafka producer for publishing transaction events.
    
    Ensures:
    - Reliable message delivery (acks=all)
    - Proper partitioning by user_id (maintains order)
    - JSON serialization
    - Error handling and retries
    """
    
    def __init__(self):
        """Initialize Kafka producer (connection happens in connect())."""
        self.producer = None
        self.brokers = settings.kafka_brokers.split(",")
    
    def connect(self):
        """
        Establish connection to Kafka cluster.
        
        Called during FastAPI startup.
        
        Configuration:
        - acks='all': Wait for all replicas to acknowledge (reliable)
        - retries=3: Retry failed sends 3 times
        - value_serializer: Serialize Python dicts to JSON
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.brokers,
                acks='all',  # Wait for all replicas (most reliable)
                retries=3,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type='gzip',  # Compress messages to save bandwidth
            )
            logger.info(f"✓ Connected to Kafka brokers: {self.brokers}")
        except Exception as e:
            logger.error(f"✗ Failed to connect to Kafka: {e}")
            raise
    
    def send(
        self,
        topic: str,
        message: Dict[str, Any],
        partition_key: Optional[str] = None,
    ) -> str:
        """
        Publish a message to Kafka topic.
        
        Args:
            topic: Kafka topic name (e.g., 'transactions')
            message: Dictionary to serialize as JSON
            partition_key: Key for partitioning (e.g., user_id)
                          Messages with same key go to same partition
        
        Returns:
            str: Message ID for tracking
        
        Raises:
            Exception: If publish fails after retries
        
        Example:
            kafka_producer.send(
                topic='transactions',
                message={'user_id': 123, 'amount': 100},
                partition_key='123',  # All user 123 transactions go to same partition
            )
        """
        
        if not self.producer:
            raise RuntimeError("Kafka producer not connected. Call connect() first.")
        
        try:
            # Convert partition_key to bytes if provided
            key = partition_key.encode('utf-8') if partition_key else None
            
            # Send to Kafka
            # send() is async - returns Future
            future = self.producer.send(
                topic,
                value=message,
                key=key,
            )
            
            # Wait for send to complete (timeout=10 seconds)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"✓ Published to {topic} "
                f"[partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}]"
            )
            
            # Return message ID for tracking
            return f"{record_metadata.partition}:{record_metadata.offset}"
        
        except Exception as e:
            logger.error(f"✗ Failed to publish to Kafka: {e}")
            raise
    
    def close(self):
        """
        Close Kafka connection.
        
        Called during FastAPI shutdown.
        Ensures all pending messages are flushed.
        """
        if self.producer:
            try:
                self.producer.flush(timeout=10)  # Wait for all messages to send
                self.producer.close()
                logger.info("✓ Kafka producer closed")
            except Exception as e:
                logger.error(f"✗ Error closing Kafka producer: {e}")


# Global instance (singleton pattern)
# Used throughout the application
_producer_instance = None


def get_kafka_producer() -> KafkaProducer:
    """
    Get global Kafka producer instance.
    
    Singleton pattern ensures only one connection to Kafka.
    """
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = KafkaProducer()
    return _producer_instance