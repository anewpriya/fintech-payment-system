"""
Settlement service - processes approved transactions.

Handles:
- Account balance updates (optimistic locking)
- Transaction reversals
- Audit logging
- Error recovery
"""

import logging
from typing import Dict, Any, Optional
from decimal import Decimal
from datetime import datetime
import uuid

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from src.models import Transaction, Settlement, UserAccount, AuditLog
from src.database import SessionLocal

logger = logging.getLogger(__name__)


class SettlementService:
    """
    Handles settlement of approved transactions.
    
    Responsible for:
    1. Updating user/merchant balances
    2. Recording settlement in database
    3. Creating audit logs
    4. Handling reversals
    """
    
    def __init__(self):
        """Initialize settlement service."""
        self.db = SessionLocal()
    
    def settle_transaction(
        self,
        transaction_id: str,
        fraud_decision: str,  # APPROVE or DECLINE
    ) -> Dict[str, Any]:
        """
        Settle an approved transaction.
        
        Args:
            transaction_id: UUID of transaction
            fraud_decision: APPROVE or DECLINE from fraud detector
        
        Returns:
            {
                'settlement_id': 'uuid-...',
                'status': 'SETTLED' or 'DECLINED',
                'user_balance_after': 900.00,
                'merchant_balance_after': 50098.00,
                'processing_time_ms': 45,
            }
        """
        
        import time
        start_time = time.time()
        
        try:
            # Step 1: Fetch transaction from database
            transaction = self.db.query(Transaction).filter(
                Transaction.transaction_id == transaction_id
            ).first()
            
            if not transaction:
                logger.error(f"Transaction {transaction_id} not found")
                return {
                    'settlement_id': None,
                    'status': 'ERROR',
                    'error': 'Transaction not found',
                }
            
            # Step 2: If DECLINE, don't settle (just update status)
            if fraud_decision == 'DECLINE':
                logger.info(f"Transaction {transaction_id} declined by fraud detector")
                
                transaction.status = 'DECLINED'
                self.db.commit()
                
                # Log in audit
                self._log_audit(
                    entity_type='transaction',
                    entity_id=transaction.transaction_id,
                    action='declined',
                    old_values={'status': 'PENDING'},
                    new_values={'status': 'DECLINED'},
                    system_service='settlement_service',
                )
                
                processing_time_ms = (time.time() - start_time) * 1000
                
                return {
                    'settlement_id': None,
                    'status': 'DECLINED',
                    'processing_time_ms': processing_time_ms,
                }
            
            # Step 3: APPROVE - proceed with settlement
            if fraud_decision != 'APPROVE':
                logger.error(f"Invalid fraud_decision: {fraud_decision}")
                return {
                    'settlement_id': None,
                    'status': 'ERROR',
                    'error': f'Invalid decision: {fraud_decision}',
                }
            
            logger.info(f"Settling approved transaction {transaction_id}")
            
            # Step 4: Update user balance (optimistic locking)
            user_updated = self._deduct_from_user(
                user_id=transaction.user_id,
                amount=transaction.amount,
            )
            
            if not user_updated:
                logger.error(f"Failed to deduct from user {transaction.user_id}")
                self.db.rollback()
                return {
                    'settlement_id': None,
                    'status': 'ERROR',
                    'error': 'Failed to update user balance',
                }
            
            # Fetch updated user balance
            user_account = self.db.query(UserAccount).filter(
                UserAccount.user_id == transaction.user_id
            ).first()
            user_balance_after = user_account.balance if user_account else 0
            
            # Step 5: Update merchant balance
            processing_fee = transaction.amount * Decimal('0.02')  # 2% fee
            merchant_credit = transaction.amount - processing_fee
            
            merchant_updated = self._credit_to_merchant(
                merchant_id=transaction.merchant_id,
                amount=merchant_credit,
            )
            
            if not merchant_updated:
                logger.error(f"Failed to credit merchant {transaction.merchant_id}")
                # Rollback user deduction
                self._refund_to_user(transaction.user_id, transaction.amount)
                self.db.rollback()
                return {
                    'settlement_id': None,
                    'status': 'ERROR',
                    'error': 'Failed to update merchant balance',
                }
            
            # Fetch updated merchant balance
            merchant_account = self.db.query(UserAccount).filter(
                UserAccount.user_id == transaction.merchant_id
            ).first()
            merchant_balance_after = merchant_account.balance if merchant_account else 0
            
            # Step 6: Create settlement record
            settlement = Settlement(
                settlement_id=uuid.uuid4(),
                transaction_id=transaction.transaction_id,
                created_at=datetime.utcnow(),
                settled_at=datetime.utcnow(),
                gross_amount=transaction.amount,
                processing_fee=processing_fee,
                net_amount=merchant_credit,
                user_id=transaction.user_id,
                merchant_id=transaction.merchant_id,
                status='SETTLED',
                posted_to_bank=False,
            )
            
            self.db.add(settlement)
            
            # Step 7: Update transaction status
            transaction.status = 'APPROVED'
            
            # Step 8: Log audit trail
            self._log_audit(
                entity_type='transaction',
                entity_id=transaction.transaction_id,
                action='settled',
                old_values={'status': 'PENDING'},
                new_values={'status': 'APPROVED'},
                system_service='settlement_service',
            )
            
            self._log_audit(
                entity_type='settlement',
                entity_id=settlement.settlement_id,
                action='created',
                old_values=None,
                new_values={
                    'gross_amount': str(settlement.gross_amount),
                    'processing_fee': str(settlement.processing_fee),
                    'net_amount': str(settlement.net_amount),
                },
                system_service='settlement_service',
            )
            
            # Step 9: Commit all changes
            self.db.commit()
            
            logger.info(
                f"✓ Settlement complete for {transaction_id}: "
                f"user balance={user_balance_after}, "
                f"merchant balance={merchant_balance_after}"
            )
            
            processing_time_ms = (time.time() - start_time) * 1000
            
            return {
                'settlement_id': str(settlement.settlement_id),
                'status': 'SETTLED',
                'user_balance_after': float(user_balance_after),
                'merchant_balance_after': float(merchant_balance_after),
                'processing_fee': float(processing_fee),
                'net_amount': float(merchant_credit),
                'processing_time_ms': processing_time_ms,
            }
        
        except Exception as e:
            logger.error(f"Error settling transaction: {e}", exc_info=True)
            self.db.rollback()
            return {
                'settlement_id': None,
                'status': 'ERROR',
                'error': str(e),
            }
    
    def _deduct_from_user(
        self,
        user_id: int,
        amount: Decimal,
    ) -> bool:
        """
        Deduct amount from user account using optimistic locking.
        
        Returns True if successful, False if retry needed.
        """
        
        try:
            # Get current account state
            account = self.db.query(UserAccount).filter(
                UserAccount.user_id == user_id
            ).with_for_update().first()  # Lock for update
            
            if not account:
                # Create account if doesn't exist
                account = UserAccount(
                    account_id=uuid.uuid4(),
                    user_id=user_id,
                    balance=Decimal('0'),
                    version=1,
                )
                self.db.add(account)
                self.db.flush()
            
            # Check if sufficient balance
            if account.balance < amount:
                logger.error(
                    f"Insufficient balance for user {user_id}: "
                    f"balance={account.balance}, amount={amount}"
                )
                return False
            
            # Update using optimistic locking
            old_version = account.version
            new_version = old_version + 1
            new_balance = account.balance - amount
            
            account.balance = new_balance
            account.version = new_version
            
            return True
        
        except Exception as e:
            logger.error(f"Error deducting from user {user_id}: {e}")
            return False
    
    def _credit_to_merchant(
        self,
        merchant_id: int,
        amount: Decimal,
    ) -> bool:
        """Add amount to merchant account."""
        
        try:
            account = self.db.query(UserAccount).filter(
                UserAccount.user_id == merchant_id
            ).with_for_update().first()
            
            if not account:
                # Create merchant account if doesn't exist
                account = UserAccount(
                    account_id=uuid.uuid4(),
                    user_id=merchant_id,
                    balance=Decimal('0'),
                    version=1,
                )
                self.db.add(account)
                self.db.flush()
            
            # Update balance
            account.balance = account.balance + amount
            account.version = account.version + 1
            
            return True
        
        except Exception as e:
            logger.error(f"Error crediting merchant {merchant_id}: {e}")
            return False
    
    def _refund_to_user(
        self,
        user_id: int,
        amount: Decimal,
    ):
        """Refund amount to user (reversal)."""
        
        try:
            account = self.db.query(UserAccount).filter(
                UserAccount.user_id == user_id
            ).first()
            
            if account:
                account.balance = account.balance + amount
                self.db.commit()
                logger.info(f"✓ Refunded ${amount} to user {user_id}")
        
        except Exception as e:
            logger.error(f"Error refunding user: {e}")
    
    def _log_audit(
        self,
        entity_type: str,
        entity_id: str,
        action: str,
        old_values: Optional[Dict],
        new_values: Optional[Dict],
        system_service: str,
    ):
        """Create immutable audit log entry."""
        
        try:
            audit = AuditLog(
                entity_type=entity_type,
                entity_id=entity_id,
                action=action,
                old_values=old_values,
                new_values=new_values,
                system_service=system_service,
                created_at=datetime.utcnow(),
            )
            self.db.add(audit)
        
        except Exception as e:
            logger.error(f"Error logging audit: {e}")
    
    def close(self):
        """Close database connection."""
        if self.db:
            self.db.close()


# Global instance
_settlement_service_instance = None


def get_settlement_service() -> SettlementService:
    """Get global settlement service instance."""
    global _settlement_service_instance
    if _settlement_service_instance is None:
        _settlement_service_instance = SettlementService()
    return _settlement_service_instance