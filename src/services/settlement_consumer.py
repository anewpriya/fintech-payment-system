"""
Kafka consumer for settlement processing.

Consumes fraud_scores from Kafka, settles approved transactions,
and publishes settlement results.
"""

import logging
import json
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, Any

from src.config import settings
from src.services.settlement import get_settlement_service

logger = logging.getLogger(__name__)


class SettlementConsumer:
    """
    Kafka consumer for transaction settlement.
    
    Flow:
    1. Consume fraud_scores from Kafka topic: 'fraud-scores'
    2. For APPROVE decisions, settle transaction
    3. Publish settlement results to Kafka topic: 'settlements'
    """
    
    def __init__(self):
        """Initialize settlement consumer."""
        self.consumer = None
        self.producer = None
        self.settlement_service = get_settlement_service()
        
        self.brokers = settings.kafka_brokers.split(",")
        self.fraud_scores_topic = settings.kafka_fraud_scores_topic
        self.settlements_topic = settings.kafka_settlements_topic
        self.consumer_group = f"{settings.kafka_consumer_group}-settlement"
    
    def connect(self):
        """Connect to Kafka."""
        try:
            # Consumer for fraud_scores
            self.consumer = KafkaConsumer(
                self.fraud_scores_topic,
                bootstrap_servers=self.brokers,
                group_id=self.consumer_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=100,
            )
            logger.info(f"✓ Connected to Kafka topic: {self.fraud_scores_topic}")
            
            # Producer for settlements
            self.producer = KafkaProducer(
                bootstrap_servers=self.brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
            )
            logger.info("✓ Kafka producer initialized")
        
        except Exception as e:
            logger.error(f"✗ Connection error: {e}")
            raise
    
    def process_fraud_decision(
        self,
        fraud_score: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Process fraud decision and settle if approved.
        
        Args:
            fraud_score: Fraud detection result from Kafka
                {
                    'transaction_id': 'uuid-...',
                    'fraud_score': 45.5,
                    'decision': 'APPROVE' or 'DECLINE',
                    ...
                }
        
        Returns:
            Settlement result
        """
        
        try:
            transaction_id = fraud_score['transaction_id']
            decision = fraud_score['decision']
            
            logger.info(
                f"Processing fraud decision: {transaction_id} → {decision}"
            )
            
            # Call settlement service
            settlement_result = self.settlement_service.settle_transaction(
                transaction_id=transaction_id,
                fraud_decision=decision,
            )
            
            logger.info(
                f"Settlement complete: {transaction_id} → {settlement_result['status']}"
            )
            
            return settlement_result
        
        except Exception as e:
            logger.error(f"Error processing fraud decision: {e}")
            return {
                'settlement_id': None,
                'status': 'ERROR',
                'error': str(e),
            }
    
    def publish_settlement_result(
        self,
        settlement_result: Dict[str, Any],
        fraud_score: Dict[str, Any],
    ):
        """
        Publish settlement result to Kafka.
        
        Args:
            settlement_result: Result from settlement service
            fraud_score: Original fraud score (for context)
        """
        
        try:
            message = {
                'transaction_id': fraud_score['transaction_id'],
                'settlement_id': settlement_result.get('settlement_id'),
                'status': settlement_result['status'],
                'user_balance_after': settlement_result.get('user_balance_after'),
                'merchant_balance_after': settlement_result.get('merchant_balance_after'),
                'fraud_decision': fraud_score['decision'],
                'fraud_score': fraud_score.get('fraud_score'),
                'processing_time_ms': settlement_result.get('processing_time_ms', 0),
            }
            
            # Publish to Kafka
            self.producer.send(
                self.settlements_topic,
                value=message,
                key=str(fraud_score['transaction_id']).encode('utf-8'),
            )
            
            logger.info(
                f"✓ Published settlement for {fraud_score['transaction_id']}"
            )
        
        except Exception as e:
            logger.error(f"Error publishing settlement result: {e}")
    
    def run(self):
        """Main loop - consume fraud decisions and settle transactions."""
        
        logger.info("Starting settlement consumer...")
        
        try:
            for message in self.consumer:
                try:
                    # Parse fraud score from Kafka
                    fraud_score = message.value
                    
                    logger.info(
                        f"Processing fraud decision: {fraud_score['transaction_id']} "
                        f"({fraud_score['decision']})"
                    )
                    
                    # Process and settle
                    settlement_result = self.process_fraud_decision(fraud_score)
                    
                    # Publish result
                    self.publish_settlement_result(settlement_result, fraud_score)
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
        
        except KeyboardInterrupt:
            logger.info("Shutting down settlement consumer...")
        
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
            
            if self.settlement_service:
                self.settlement_service.close()
                logger.info("✓ Settlement service closed")
        
        except Exception as e:
            logger.error(f"Error closing connections: {e}")


# Global instance
_consumer_instance = None


def get_settlement_consumer() -> SettlementConsumer:
    """Get global settlement consumer instance."""
    global _consumer_instance
    if _consumer_instance is None:
        _consumer_instance = SettlementConsumer()
    return _consumer_instance