"""
Main fraud detection service orchestrator.

Coordinates Tier 1 (rules) and Tier 2 (ML) detection.
Publishes fraud decisions to Kafka.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
import json

from src.services.fraud_rules import FraudRulesEngine
from src.services.fraud_ml import get_ml_detector
from src.config import settings

logger = logging.getLogger(__name__)


class FraudDetectionService:
    """
    Orchestrates fraud detection pipeline.
    
    Flow:
    1. Receive transaction from Kafka
    2. Run Tier 1 rules (< 1ms)
    3. If uncertain, run Tier 2 ML (< 50ms)
    4. Make decision: APPROVE or DECLINE
    5. Publish fraud_scores to Kafka
    """
    
    def __init__(self):
        """Initialize fraud detection service."""
        self.rules_engine = FraudRulesEngine()
        self.ml_detector = get_ml_detector()
    
    def detect_fraud(
        self,
        transaction: Dict[str, Any],
        features: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Detect fraud in transaction.
        
        Args:
            transaction: Transaction details from Kafka
                {
                    'transaction_id': 'uuid-...',
                    'user_id': 12345,
                    'amount': 100.00,
                    'user_country': 'US',
                    'card_hash': 'abc123...',
                    'device_fingerprint': 'dev-xyz',
                }
            
            features: Pre-computed features from Redis
                {
                    'transactions_1min': 2,
                    'transactions_1hour': 45,
                    'avg_transaction_amount': 50.00,
                    'device_trust_score': 85,
                    ...
                }
        
        Returns:
            {
                'transaction_id': 'uuid-...',
                'fraud_score': 45.5,
                'fraud_tier': 1 or 2,
                'decision': 'APPROVE' or 'DECLINE',
                'reasoning': 'High velocity from new device',
                'processing_time_ms': 48,
            }
        """
        
        import time
        start_time = time.time()
        
        try:
            # ================================================================
            # TIER 1: Rule-Based Detection
            # ================================================================
            
            logger.info(
                f"[TIER 1] Checking rules for transaction {transaction['transaction_id']}"
            )
            
            tier1_result = self.rules_engine.check_all_rules(
                user_id=transaction['user_id'],
                card_hash=transaction['card_hash'],
                amount=transaction['amount'],
                user_country=transaction['user_country'],
                device_fingerprint=transaction.get('device_fingerprint'),
            )
            
            # Tier 1 returned True = fraud detected
            if tier1_result is True:
                logger.warning(
                    f"[TIER 1] FRAUD DETECTED: {transaction['transaction_id']}"
                )
                
                processing_time_ms = (time.time() - start_time) * 1000
                
                return {
                    'transaction_id': transaction['transaction_id'],
                    'fraud_score': 100,  # Definite fraud
                    'fraud_tier': 1,  # Caught by Tier 1
                    'decision': 'DECLINE',
                    'reasoning': 'Fraud detected by rules (stolen card, sanctioned country, or impossible travel)',
                    'processing_time_ms': processing_time_ms,
                }
            
            # Tier 1 returned False = definitely not fraud (rare)
            if tier1_result is False:
                logger.info(
                    f"[TIER 1] APPROVED: {transaction['transaction_id']}"
                )
                
                processing_time_ms = (time.time() - start_time) * 1000
                
                return {
                    'transaction_id': transaction['transaction_id'],
                    'fraud_score': 0,  # Definitely not fraud
                    'fraud_tier': 1,  # Passed all rule checks
                    'decision': 'APPROVE',
                    'reasoning': 'Passed all Tier 1 rule checks',
                    'processing_time_ms': processing_time_ms,
                }
            
            # Tier 1 returned None = uncertain, escalate to Tier 2
            logger.info(
                f"[TIER 1] UNCERTAIN: {transaction['transaction_id']} → escalating to Tier 2"
            )
            
            # ================================================================
            # TIER 2: Machine Learning Detection
            # ================================================================
            
            logger.info(
                f"[TIER 2] Running ML model for transaction {transaction['transaction_id']}"
            )
            
            ml_result = self.ml_detector.score_transaction(
                transaction_data=transaction,
                features=features,
            )
            
            fraud_score = ml_result['fraud_score']
            reasoning = ml_result['reasoning']
            
            # Make decision based on ML score
            if fraud_score >= settings.fraud_threshold:  # Default: 75
                decision = 'DECLINE'
                logger.warning(
                    f"[TIER 2] FRAUD SUSPECTED: {transaction['transaction_id']} "
                    f"(score: {fraud_score})"
                )
            else:
                decision = 'APPROVE'
                logger.info(
                    f"[TIER 2] APPROVED: {transaction['transaction_id']} "
                    f"(score: {fraud_score})"
                )
            
            processing_time_ms = (time.time() - start_time) * 1000
            
            return {
                'transaction_id': transaction['transaction_id'],
                'fraud_score': fraud_score,
                'fraud_tier': 2,  # Caught by Tier 2
                'decision': decision,
                'reasoning': reasoning,
                'processing_time_ms': processing_time_ms,
            }
        
        except Exception as e:
            logger.error(
                f"Error detecting fraud for {transaction['transaction_id']}: {e}"
            )
            
            # Default to declining on error (safe fallback)
            processing_time_ms = (time.time() - start_time) * 1000
            
            return {
                'transaction_id': transaction['transaction_id'],
                'fraud_score': 50,  # Medium risk (error)
                'fraud_tier': None,
                'decision': 'DECLINE',
                'reasoning': f'Error in fraud detection: {str(e)}',
                'processing_time_ms': processing_time_ms,
            }


# Global instance (singleton)
_fraud_service_instance = None


def get_fraud_detection_service() -> FraudDetectionService:
    """Get global fraud detection service instance."""
    global _fraud_service_instance
    if _fraud_service_instance is None:
        _fraud_service_instance = FraudDetectionService()
    return _fraud_service_instance