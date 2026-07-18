"""
Kafka consumer for fraud detection.

Consumes transactions from Kafka, runs fraud detection,
and publishes fraud scores back to Kafka.
"""

import logging
import json
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, Any
import redis

from src.config import settings
from src.services.fraud_detector import get_fraud_detection_service

logger = logging.getLogger(__name__)


class FraudDetectionConsumer:
    """
    Kafka consumer for real-time fraud detection.
    
    Flow:
    1. Consume transaction from Kafka topic: 'transactions'
    2. Fetch features from Redis
    3. Run fraud detection (Tier 1 + Tier 2)
    4. Publish fraud_scores to Kafka topic: 'fraud-scores'
    """
    
    def __init__(self):
        """Initialize fraud detection consumer."""
        self.consumer = None
        self.producer = None
        self.redis_client = None
        self.fraud_service = get_fraud_detection_service()
        
        self.brokers = settings.kafka_brokers.split(",")
        self.transactions_topic = settings.kafka_transactions_topic
        self.fraud_scores_topic = settings.kafka_fraud_scores_topic
        self.consumer_group = settings.kafka_consumer_group
    
    def connect(self):
        """
        Connect to Kafka and Redis.
        
        Called during application startup.
        """
        try:
            # Connect to Kafka Consumer
            self.consumer = KafkaConsumer(
                self.transactions_topic,
                bootstrap_servers=self.brokers,
                group_id=self.consumer_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Start from beginning if no offset
                enable_auto_commit=True,  # Auto-commit after processing
                max_poll_records=100,  # Batch size
            )
            logger.info(f"✓ Connected to Kafka topic: {self.transactions_topic}")
            
            # Connect to Kafka Producer (for publishing fraud_scores)
            self.producer = KafkaProducer(
                bootstrap_servers=self.brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas
            )
            logger.info("✓ Kafka producer initialized")
            
            # Connect to Redis (for features)
            self.redis_client = redis.from_url(settings.redis_url)
            self.redis_client.ping()
            logger.info("✓ Connected to Redis")
        
        except Exception as e:
            logger.error(f"✗ Connection error: {e}")
            raise
    
    def get_features_from_redis(self, user_id: int) -> Dict[str, Any]:
        """
        Fetch pre-computed features from Redis.
        
        Features are computed by Flink stream processing in real-time.
        This returns the latest features for the user.
        
        Args:
            user_id: User ID to fetch features for
        
        Returns:
            Dictionary of features, or defaults if not found
        """
        
        try:
            # Key format: user_velocity:{user_id}
            key = f"user_velocity:{user_id}"
            
            # Get from Redis
            value = self.redis_client.get(key)
            
            if value:
                features = json.loads(value)
                logger.debug(f"✓ Fetched features for user {user_id}")
                return features
            else:
                # No features yet (new user), return defaults
                logger.debug(f"No features found for user {user_id}, using defaults")
                return self._default_features()
        
        except Exception as e:
            logger.error(f"Error fetching features from Redis: {e}")
            return self._default_features()
    
    def _default_features(self) -> Dict[str, Any]:
        """
        Return default features for new/unknown users.
        
        Slightly conservative (medium risk) for unknowns.
        """
        return {
            'transactions_1min': 0,
            'transactions_5min': 0,
            'transactions_1hour': 0,
            'transactions_24hour': 0,
            'amount_1min': 0,
            'amount_1hour': 0,
            'amount_24hour': 0,
            'avg_transaction_amount': 50,
            'device_trust_score': 50,  # Medium trust for unknown device
            'is_familiar_merchant': False,  # Assume unknown merchant
            'is_normal_location': True,  # Assume normal location
        }
    
    def process_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process single transaction through fraud detection.
        
        Args:
            transaction: Transaction data from Kafka
        
        Returns:
            Fraud detection result with score and decision
        """
        
        try:
            # Step 1: Fetch features from Redis
            features = self.get_features_from_redis(transaction['user_id'])
            
            # Step 2: Add transaction amount to features (for scoring)
            features['amount'] = transaction['amount']
            
            # Step 3: Run fraud detection
            result = self.fraud_service.detect_fraud(
                transaction=transaction,
                features=features,
            )
            
            logger.info(
                f"Fraud detection complete: {result['transaction_id']} → "
                f"{result['decision']} (score: {result['fraud_score']}, "
                f"latency: {result['processing_time_ms']:.1f}ms)"
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error processing transaction: {e}")
            # Return error result
            return {
                'transaction_id': transaction.get('transaction_id'),
                'fraud_score': 50,
                'fraud_tier': None,
                'decision': 'DECLINE',
                'reasoning': f'Processing error: {str(e)}',
                'processing_time_ms': 0,
            }
    
    def publish_fraud_score(self, fraud_result: Dict[str, Any]):
        """
        Publish fraud detection result to Kafka.
        
        Args:
            fraud_result: Result from fraud detection
        """
        
        try:
            # Prepare message for Kafka
            message = {
                'transaction_id': fraud_result['transaction_id'],
                'fraud_score': fraud_result['fraud_score'],
                'fraud_tier': fraud_result['fraud_tier'],
                'decision': fraud_result['decision'],
                'reasoning': fraud_result['reasoning'],
                'processing_time_ms': fraud_result['processing_time_ms'],
                'timestamp': json.dumps(None, default=str),  # Current timestamp
            }
            
            # Publish to Kafka (partition by transaction_id)
            self.producer.send(
                self.fraud_scores_topic,
                value=message,
                key=str(fraud_result['transaction_id']).encode('utf-8'),
            )
            
            logger.info(
                f"✓ Published fraud_score for {fraud_result['transaction_id']}"
            )
        
        except Exception as e:
            logger.error(f"Error publishing fraud score: {e}")
    
    def run(self):
        """
        Main loop - consume transactions and detect fraud.
        
        This runs continuously, processing transactions as they arrive.
        """
        
        logger.info("Starting fraud detection consumer...")
        
        try:
            for message in self.consumer:
                try:
                    # Parse transaction from Kafka
                    transaction = message.value
                    
                    logger.info(
                        f"Processing transaction: {transaction['transaction_id']} "
                        f"(user: {transaction['user_id']}, amount: ${transaction['amount']})"
                    )
                    
                    # Run fraud detection
                    fraud_result = self.process_transaction(transaction)
                    
                    # Publish fraud_scores to Kafka
                    self.publish_fraud_score(fraud_result)
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Continue processing next message
                    continue
        
        except KeyboardInterrupt:
            logger.info("Shutting down fraud detection consumer...")
        
        except Exception as e:
            logger.error(f"Fatal error in consumer loop: {e}")
        
        finally:
            self.close()
    
    def close(self):
        """Close all connections."""
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("✓ Kafka consumer closed")
            
            if self.producer:
                self.producer.close()
                logger.info("✓ Kafka producer closed")
            
            if self.redis_client:
                self.redis_client.close()
                logger.info("✓ Redis connection closed")
        
        except Exception as e:
            logger.error(f"Error closing connections: {e}")


# Global instance
_consumer_instance = None


def get_fraud_consumer() -> FraudDetectionConsumer:
    """Get global fraud detection consumer instance."""
    global _consumer_instance
    if _consumer_instance is None:
        _consumer_instance = FraudDetectionConsumer()
    return _consumer_instance