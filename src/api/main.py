"""
FastAPI application - Entry point for the fintech payment system.

This module defines the API Gateway that receives transactions,
validates them, and publishes to Kafka for processing.
"""

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import Optional
import uuid
from datetime import datetime
import logging

from src.config import settings
from src.database import get_db, init_db, close_db
from src.models import Transaction
from src.api.kafka_producer import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Fintech Payment System",
    description="Production-grade payment processing with real-time fraud detection",
    version="0.1.0",
)

# Initialize Kafka producer
kafka_producer = KafkaProducer()


# ============================================================================
# Request/Response Models (Pydantic)
# ============================================================================

class TransactionRequest(BaseModel):
    """
    Schema for incoming transaction request from user.
    
    Pydantic validates the request:
    - amount must be > 0
    - currency must be 3 chars
    - card_last_four must be exactly 4 digits
    """
    
    user_id: int = Field(..., gt=0, description="User ID")
    merchant_id: int = Field(..., gt=0, description="Merchant ID")
    user_email: str = Field(..., description="User email address")
    user_country: str = Field(..., min_length=2, max_length=2, description="ISO country code")
    
    amount: float = Field(..., gt=0, description="Transaction amount")
    currency: str = Field(default="USD", min_length=3, max_length=3, description="ISO currency code")
    description: Optional[str] = Field(None, description="Transaction description")
    
    card_last_four: str = Field(..., min_length=4, max_length=4, description="Last 4 digits of card")
    card_brand: str = Field(..., description="Card brand (VISA, MASTERCARD, AMEX)")
    card_hash: str = Field(..., description="SHA256 hash of full card number")
    
    device_fingerprint: Optional[str] = Field(None, description="Device fingerprint")
    ip_address: Optional[str] = Field(None, description="Customer IP address")
    user_agent: Optional[str] = Field(None, description="User agent string")
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": 12345,
                "merchant_id": 67890,
                "user_email": "alice@example.com",
                "user_country": "US",
                "amount": 100.00,
                "currency": "USD",
                "description": "Coffee purchase",
                "card_last_four": "4242",
                "card_brand": "VISA",
                "card_hash": "abc123def456...",
                "device_fingerprint": "device-xyz",
                "ip_address": "192.168.1.1",
                "user_agent": "Mozilla/5.0...",
            }
        }


class TransactionResponse(BaseModel):
    """Response returned to user after transaction submission."""
    
    transaction_id: str
    idempotency_key: str
    status: str
    message: str
    timestamp: str


# ============================================================================
# Startup/Shutdown Events
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """
    Called when FastAPI application starts.
    
    Initializes:
    - Database connection and tables
    - Kafka producer connection
    """
    logger.info("Starting Fintech Payment System API...")
    
    try:
        # Initialize database
        init_db()
        logger.info("✓ Database initialized")
        
        # Initialize Kafka producer
        kafka_producer.connect()
        logger.info("✓ Kafka producer connected")
        
        logger.info("✓ Application startup complete")
    except Exception as e:
        logger.error(f"✗ Startup failed: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Called when FastAPI application shuts down."""
    logger.info("Shutting down Fintech Payment System API...")
    
    try:
        # Close Kafka producer
        kafka_producer.close()
        logger.info("✓ Kafka producer closed")
        
        # Close database connections
        close_db()
        logger.info("✓ Database connections closed")
        
        logger.info("✓ Application shutdown complete")
    except Exception as e:
        logger.error(f"✗ Shutdown error: {e}")


# ============================================================================
# Health Check Endpoint
# ============================================================================

@app.get("/health")
async def health_check():
    """
    Health check endpoint for monitoring.
    
    Returns 200 OK if service is healthy.
    Used by load balancers and monitoring systems.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "fintech-api",
        "version": "0.1.0",
    }


# ============================================================================
# Transaction Endpoints
# ============================================================================

@app.post(
    "/transactions",
    response_model=TransactionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a transaction",
    description="Submit a new transaction for processing. Returns 202 ACCEPTED immediately.",
)
async def create_transaction(
    request: TransactionRequest,
    db: Session = Depends(get_db),
):
    """
    Create a new transaction.
    
    This endpoint:
    1. Validates the request (Pydantic)
    2. Creates transaction record in PostgreSQL
    3. Publishes to Kafka for async processing
    4. Returns immediately (202 ACCEPTED)
    
    The actual fraud detection and settlement happen asynchronously
    via Kafka consumers.
    
    Args:
        request: Transaction details from user
        db: Database session (injected by FastAPI)
    
    Returns:
        TransactionResponse with transaction_id and status
    
    Raises:
        HTTPException: If database error or Kafka publish fails
    """
    
    try:
        # Generate IDs
        transaction_id = uuid.uuid4()
        idempotency_key = uuid.uuid4()
        
        logger.info(f"Processing transaction {transaction_id} for user {request.user_id}")
        
        # Step 1: Create transaction record in PostgreSQL
        # Status = PENDING (not yet approved/declined)
        transaction = Transaction(
            transaction_id=transaction_id,
            idempotency_key=idempotency_key,
            user_id=request.user_id,
            merchant_id=request.merchant_id,
            user_email=request.user_email,
            user_country=request.user_country,
            amount=request.amount,
            currency=request.currency,
            description=request.description,
            card_last_four=request.card_last_four,
            card_brand=request.card_brand,
            card_hash=request.card_hash,
            device_fingerprint=request.device_fingerprint,
            ip_address=request.ip_address,
            user_agent=request.user_agent,
            status="PENDING",  # Will be updated by fraud detection service
        )
        
        # Save to database
        db.add(transaction)
        db.commit()
        db.refresh(transaction)
        logger.info(f"✓ Transaction {transaction_id} saved to database")
        
        # Step 2: Publish to Kafka for async processing
        # Fraud detection service will consume this
        kafka_message = {
            "transaction_id": str(transaction_id),
            "idempotency_key": str(idempotency_key),
            "user_id": request.user_id,
            "merchant_id": request.merchant_id,
            "user_email": request.user_email,
            "user_country": request.user_country,
            "amount": float(request.amount),
            "currency": request.currency,
            "card_last_four": request.card_last_four,
            "card_brand": request.card_brand,
            "card_hash": request.card_hash,
            "device_fingerprint": request.device_fingerprint,
            "ip_address": request.ip_address,
            "user_agent": request.user_agent,
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        # Publish with partition key = user_id (ensures ordering per user)
        kafka_producer.send(
            topic=settings.kafka_transactions_topic,
            message=kafka_message,
            partition_key=str(request.user_id),
        )
        logger.info(f"✓ Transaction {transaction_id} published to Kafka")
        
        # Step 3: Return response immediately (202 ACCEPTED)
        # Fraud detection happens asynchronously
        return TransactionResponse(
            transaction_id=str(transaction_id),
            idempotency_key=str(idempotency_key),
            status="PENDING",
            message="Transaction received and is being processed",
            timestamp=datetime.utcnow().isoformat(),
        )
    
    except Exception as e:
        logger.error(f"✗ Error processing transaction: {e}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to process transaction",
        )


@app.get(
    "/transactions/{transaction_id}",
    summary="Get transaction status",
    description="Retrieve the current status of a transaction",
)
async def get_transaction(
    transaction_id: str,
    db: Session = Depends(get_db),
):
    """
    Retrieve transaction status.
    
    Args:
        transaction_id: UUID of the transaction
        db: Database session
    
    Returns:
        Transaction details including current status and fraud score
    """
    
    try:
        transaction = db.query(Transaction).filter(
            Transaction.transaction_id == transaction_id
        ).first()
        
        if not transaction:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Transaction not found",
            )
        
        return {
            "transaction_id": str(transaction.transaction_id),
            "status": transaction.status,
            "fraud_score": transaction.fraud_score,
            "fraud_tier": transaction.fraud_tier,
            "amount": str(transaction.amount),
            "created_at": transaction.created_at.isoformat(),
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"✗ Error retrieving transaction: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve transaction",
        )


# ============================================================================
# Metrics Endpoint (for Prometheus monitoring)
# ============================================================================

@app.get("/metrics")
async def metrics():
    """
    Prometheus metrics endpoint.
    
    Returns application metrics like transaction counts, latencies, etc.
    Scraped by Prometheus for monitoring and alerting.
    """
    return {
        "status": "metrics_available",
        "note": "Implement prometheus_client metrics collection here",
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
    )