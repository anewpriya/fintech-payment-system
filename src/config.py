"""
Configuration management for the fintech payment system.

This module loads environment variables and provides them as a singleton
configuration object that can be imported throughout the application.
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database Configuration
    database_url: str = "postgresql://postgres:password@localhost:5432/fintech_db"
    database_pool_size: int = 20
    database_max_overflow: int = 10
    
    # Kafka Configuration
    kafka_brokers: str = "localhost:9092"
    kafka_consumer_group: str = "fintech-service"
    kafka_transactions_topic: str = "transactions"
    kafka_fraud_scores_topic: str = "fraud-scores"
    kafka_settlements_topic: str = "settlements"
    
    # Redis Configuration
    redis_url: str = "redis://localhost:6379/0"
    redis_key_prefix: str = "fintech:"
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = True
    
    # Fraud Detection
    fraud_threshold: int = 75  # Score above this = decline
    fraud_tier2_timeout_ms: int = 50
    
    # Monitoring
    prometheus_port: int = 8001
    
    class Config:
        """Pydantic settings configuration."""
        env_file = ".env"
        case_sensitive = False


# Create singleton instance
settings = Settings()