"""
Machine learning fraud detection (Tier 2).

Uses LightGBM model to score transactions based on features.
Catches subtle fraud patterns that rules don't catch.
"""

import logging
from typing import Dict, Any, Optional
import json

logger = logging.getLogger(__name__)


class MLFraudDetector:
    """
    Machine learning-based fraud detection (Tier 2).
    
    Scores transactions based on 40+ features:
    - Velocity features (how many transactions today?)
    - Behavioral features (spending pattern changed?)
    - Device features (new device?)
    - Network features (suspicious IP?)
    
    Outputs fraud_score (0-100):
    - 0-30: Low risk (APPROVE)
    - 30-75: Medium risk (APPROVE but monitor)
    - 75-100: High risk (DECLINE)
    """
    
    def __init__(self):
        """Initialize ML detector."""
        # In production, load trained model from file
        # import lightgbm as lgb
        # self.model = lgb.Booster(model_file='fraud_model.pkl')
        
        # For this portfolio project, we simulate with rules
        # (Real implementation would have actual LightGBM model)
        self.model = None
        self.feature_threshold = 0.5  # Threshold for positive class
    
    def score_transaction(
        self,
        transaction_data: Dict[str, Any],
        features: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Score transaction using ML model.
        
        Args:
            transaction_data: Transaction details (amount, user_id, etc.)
            features: Pre-computed features from Redis (velocity, behavioral, etc.)
        
        Returns:
            {
                'fraud_score': 45.5,  # 0-100
                'fraud_probability': 0.455,  # 0-1
                'reasoning': 'High velocity from new device',
                'processing_time_ms': 23
            }
        """
        
        try:
            import time
            start_time = time.time()
            
            # Step 1: Extract features into format model expects
            feature_vector = self._prepare_feature_vector(transaction_data, features)
            
            # Step 2: Score with model
            # In production: prediction = self.model.predict([feature_vector])
            # For now, simulate with heuristic scoring
            fraud_score = self._simulate_model_scoring(feature_vector, features)
            
            # Step 3: Convert to probability
            fraud_probability = fraud_score / 100.0
            
            # Step 4: Generate reasoning
            reasoning = self._generate_reasoning(features, fraud_score)
            
            # Step 5: Calculate latency
            processing_time_ms = (time.time() - start_time) * 1000
            
            logger.info(
                f"ML Fraud Score: {fraud_score:.1f} "
                f"(latency: {processing_time_ms:.1f}ms)"
            )
            
            return {
                'fraud_score': fraud_score,
                'fraud_probability': fraud_probability,
                'reasoning': reasoning,
                'processing_time_ms': processing_time_ms,
            }
        
        except Exception as e:
            logger.error(f"Error scoring transaction: {e}")
            # Fallback to medium risk if model fails
            return {
                'fraud_score': 50,
                'fraud_probability': 0.5,
                'reasoning': 'Model error, defaulting to medium risk',
                'processing_time_ms': 0,
            }
    
    def _prepare_feature_vector(
        self,
        transaction_data: Dict[str, Any],
        features: Dict[str, Any],
    ) -> list:
        """
        Prepare feature vector for model.
        
        In production, this would extract exact features the model expects.
        For now, it's a placeholder.
        """
        # In production: precise feature ordering that matches training
        # For now, return empty (we simulate scoring below)
        return []
    
    def _simulate_model_scoring(
        self,
        feature_vector: list,
        features: Dict[str, Any],
    ) -> float:
        """
        Simulate LightGBM model scoring.
        
        In production, this would be:
        prediction = self.model.predict([feature_vector])[0]
        fraud_score = int(prediction * 100)
        
        For this portfolio, we use heuristic scoring based on features
        to show understanding of what model would detect.
        """
        
        score = 0  # Start at 0 (no fraud)
        
        # Feature 1: Velocity (how many transactions today?)
        if features.get('transactions_1min', 0) > 10:
            score += 20  # Rapid transactions = suspicious
        if features.get('transactions_1hour', 0) > 100:
            score += 15  # Many transactions in hour = suspicious
        
        # Feature 2: Amount deviation (is amount unusual for this user?)
        avg_amount = features.get('avg_transaction_amount', 50)
        current_amount = features.get('amount', 0)
        if avg_amount > 0:
            deviation = current_amount / avg_amount
            if deviation > 5:  # Spending 5x normal = suspicious
                score += 25
            elif deviation > 3:
                score += 15
            elif deviation > 2:
                score += 10
        
        # Feature 3: Device trust (is device trusted?)
        device_trust = features.get('device_trust_score', 50)
        if device_trust < 30:  # Untrusted device
            score += 20
        elif device_trust < 50:
            score += 10
        
        # Feature 4: Merchant familiarity (is this a known merchant?)
        is_familiar_merchant = features.get('is_familiar_merchant', False)
        if not is_familiar_merchant:
            score += 15  # Unknown merchant = higher risk
        
        # Feature 5: Geographic consistency (normal location for user?)
        is_normal_location = features.get('is_normal_location', True)
        if not is_normal_location:
            score += 20  # Unusual location = suspicious
        
        # Cap at 100
        return min(score, 100)
    
    def _generate_reasoning(
        self,
        features: Dict[str, Any],
        fraud_score: float,
    ) -> str:
        """
        Generate human-readable explanation for score.
        
        Helps fraud analysts understand why transaction was flagged.
        """
        
        reasons = []
        
        if features.get('transactions_1min', 0) > 10:
            reasons.append(f"High velocity: {features['transactions_1min']} transactions in 1 min")
        
        avg_amount = features.get('avg_transaction_amount', 50)
        current_amount = features.get('amount', 0)
        if avg_amount > 0 and current_amount / avg_amount > 5:
            reasons.append(f"Spending 5x normal: ${current_amount} vs avg ${avg_amount}")
        
        device_trust = features.get('device_trust_score', 50)
        if device_trust < 30:
            reasons.append(f"Untrusted device: trust score {device_trust}/100")
        
        if not features.get('is_familiar_merchant', False):
            reasons.append("New merchant (not in user's history)")
        
        if not features.get('is_normal_location', True):
            reasons.append("Unusual location for this user")
        
        if not reasons:
            reasons.append("No specific red flags detected")
        
        return "; ".join(reasons)


# Global instance
_ml_detector_instance = None


def get_ml_detector() -> MLFraudDetector:
    """Get global ML detector instance (singleton)."""
    global _ml_detector_instance
    if _ml_detector_instance is None:
        _ml_detector_instance = MLFraudDetector()
    return _ml_detector_instance