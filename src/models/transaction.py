"""
Database models for the fintech payment system.

This module defines SQLAlchemy ORM models that correspond to our PostgreSQL tables.
Each class represents a table, and each attribute represents a column.
"""

from sqlalchemy import Column, String, DECIMAL, Integer, DateTime, Boolean, JSON, Index
from sqlalchemy.dialects.postgresql import UUID, INET
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import uuid

# Base class for all models
Base = declarative_base()


class Transaction(Base):
    """
    Represents a single transaction in the system.
    
    This is the source of truth for all transaction attempts,
    including declined, pending, and approved transactions.
    """
    
    __tablename__ = "transactions"
    
    # Identifiers
    transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    idempotency_key = Column(UUID(as_uuid=True), nullable=False, unique=True, index=True)
    
    # Timestamp
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    
    # User & Merchant
    user_id = Column(Integer, nullable=False, index=True)
    merchant_id = Column(Integer, nullable=False, index=True)
    
    # User Demographics (DENORMALIZED)
    user_email = Column(String(255), nullable=False)
    user_country = Column(String(2), nullable=False)
    
    # Transaction Details
    amount = Column(DECIMAL(19, 2), nullable=False)
    currency = Column(String(3), nullable=False, default="USD")
    description = Column(String(500))
    
    # Card Details (HASHED for PCI-DSS)
    card_last_four = Column(String(4), nullable=False)
    card_brand = Column(String(20), nullable=False)  # VISA, MASTERCARD, AMEX
    card_hash = Column(String(64), nullable=False)   # SHA256 of full card number
    
    # Risk Assessment
    fraud_score = Column(DECIMAL(5, 2))              # 0-100
    fraud_tier = Column(Integer)                      # 1=rules, 2=ML, 3=manual
    
    # Status
    status = Column(String(50), nullable=False, default="PENDING", index=True)
    # Status values: PENDING, APPROVED, DECLINED, REVERSED
    decline_reason = Column(String(255))
    
    # Metadata
    device_fingerprint = Column(String(255))
    ip_address = Column(INET)
    user_agent = Column(String(500))
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_user_created', 'user_id', 'created_at'),
        Index('idx_merchant_created', 'merchant_id', 'created_at'),
        Index('idx_fraud_score', 'fraud_score'),
        Index('idx_status', 'status'),
    )
    
    def __repr__(self):
        """String representation for debugging."""
        return f"<Transaction {self.transaction_id} | User {self.user_id} | Amount {self.amount}>"


class Settlement(Base):
    """
    Represents a confirmed settlement (transaction that actually settled).
    
    Only transactions with status=APPROVED appear here.
    This is the source of truth for accounting/reconciliation.
    """
    
    __tablename__ = "settlements"
    
    settlement_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    transaction_id = Column(UUID(as_uuid=True), nullable=False, unique=True, index=True)
    
    # When
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    settled_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Amounts
    gross_amount = Column(DECIMAL(19, 2), nullable=False)
    processing_fee = Column(DECIMAL(19, 2), nullable=False, default=0)
    net_amount = Column(DECIMAL(19, 2), nullable=False)
    
    # Who
    user_id = Column(Integer, nullable=False, index=True)
    merchant_id = Column(Integer, nullable=False, index=True)
    
    # Status
    status = Column(String(50), nullable=False, default="SETTLED")
    # Status: SETTLED, REVERSED, PENDING
    
    # Bank reconciliation
    posted_to_bank = Column(Boolean, default=False, index=True)
    bank_reference_id = Column(String(255))
    
    __table_args__ = (
        Index('idx_user_created', 'user_id', 'created_at'),
        Index('idx_merchant_created', 'merchant_id', 'created_at'),
        Index('idx_status', 'status'),
    )
    
    def __repr__(self):
        return f"<Settlement {self.settlement_id} | Amount {self.gross_amount}>"


class UserAccount(Base):
    """
    Represents a user's account balance.
    
    Uses optimistic locking (version field) to prevent race conditions
    without database locks.
    """
    
    __tablename__ = "user_accounts"
    
    account_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(Integer, nullable=False, unique=True, index=True)
    
    # Balance
    balance = Column(DECIMAL(19, 2), nullable=False, default=0)
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Optimistic locking
    version = Column(Integer, nullable=False, default=1)
    
    def __repr__(self):
        return f"<UserAccount {self.user_id} | Balance {self.balance}>"


class AuditLog(Base):
    """
    Immutable audit trail for compliance (PCI-DSS requirement).
    
    Every change to transactions/settlements/accounts is logged here.
    Can never be deleted, only marked as superseded.
    """
    
    __tablename__ = "audit_logs"
    
    audit_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # What changed
    entity_type = Column(String(50), nullable=False, index=True)
    entity_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    
    # What happened
    action = Column(String(50), nullable=False)  # created, updated, reversed
    old_values = Column(JSON)  # Previous state
    new_values = Column(JSON)  # New state
    
    # Who did it
    user_id = Column(Integer)  # If human action
    system_service = Column(String(100))  # If system action
    
    # When
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    
    __table_args__ = (
        Index('idx_entity', 'entity_type', 'entity_id'),
        Index('idx_created', 'created_at'),
    )
    
    def __repr__(self):
        return f"<AuditLog {self.audit_id} | {self.entity_type} | {self.action}>"