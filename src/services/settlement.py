"""Atomic settlement for synthetic transactions."""
import logging
import uuid
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

from src.models import Transaction, Settlement, UserAccount, AuditLog
from src.database import SessionLocal

logger = logging.getLogger(__name__)


class SettlementService:
    def settle_transaction(self, transaction_id, fraud_decision):
        if fraud_decision not in ("APPROVE", "DECLINE"):
            return {"settlement_id": None, "status": "ERROR", "error": "Invalid decision"}
        try:
            with SessionLocal.begin() as db:
                tx = db.query(Transaction).filter_by(transaction_id=uuid.UUID(str(transaction_id))).with_for_update().first()
                if tx is None:
                    return {"settlement_id": None, "status": "ERROR", "error": "Transaction not found"}
                prior = db.query(Settlement).filter_by(transaction_id=tx.transaction_id).first()
                if prior:
                    return {"settlement_id": str(prior.settlement_id), "status": "SETTLED"}
                if tx.status == "DECLINED":
                    return {"settlement_id": None, "status": "DECLINED"}
                if tx.status != "PENDING":
                    return {"settlement_id": None, "status": "ERROR", "error": "Invalid transaction state"}
                old_status = tx.status
                if fraud_decision == "DECLINE":
                    tx.status = "DECLINED"
                    db.add(AuditLog(entity_type="transaction", entity_id=tx.transaction_id,
                                    action="declined", old_values={"status": old_status},
                                    new_values={"status": tx.status}, system_service="settlement_service"))
                    return {"settlement_id": None, "status": "DECLINED"}
                if tx.currency != "USD" or tx.amount <= 0 or tx.user_id == tx.merchant_id:
                    return {"settlement_id": None, "status": "ERROR", "error": "Unsupported settlement"}
                accounts = db.query(UserAccount).filter(UserAccount.user_id.in_([tx.user_id, tx.merchant_id])).order_by(UserAccount.user_id).with_for_update().all()
                by_id = {a.user_id: a for a in accounts}
                user = by_id.get(tx.user_id)
                if user is None or user.balance < tx.amount:
                    return {"settlement_id": None, "status": "ERROR", "error": "Insufficient balance"}
                merchant = by_id.get(tx.merchant_id)
                if merchant is None:
                    merchant = UserAccount(account_id=uuid.uuid4(), user_id=tx.merchant_id, balance=Decimal("0"), version=1)
                    db.add(merchant)
                fee = (tx.amount * Decimal("0.02")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                net = tx.amount - fee
                user.balance -= tx.amount
                user.version += 1
                merchant.balance += net
                merchant.version += 1
                settlement_id = uuid.uuid4()
                db.add(Settlement(settlement_id=settlement_id, transaction_id=tx.transaction_id,
                                  gross_amount=tx.amount, processing_fee=fee, net_amount=net,
                                  user_id=tx.user_id, merchant_id=tx.merchant_id, status="SETTLED",
                                  posted_to_bank=False, settled_at=datetime.utcnow()))
                tx.status = "APPROVED"
                db.add(AuditLog(entity_type="transaction", entity_id=tx.transaction_id,
                                action="settled", old_values={"status": old_status},
                                new_values={"status": tx.status}, system_service="settlement_service"))
                db.add(AuditLog(entity_type="settlement", entity_id=settlement_id, action="created",
                                new_values={"gross_amount": str(tx.amount), "processing_fee": str(fee), "net_amount": str(net)},
                                system_service="settlement_service"))
                return {"settlement_id": str(settlement_id), "status": "SETTLED",
                        "user_balance_after": float(user.balance), "merchant_balance_after": float(merchant.balance),
                        "processing_fee": float(fee), "net_amount": float(net)}
        except Exception:
            logger.exception("Settlement failed")
            return {"settlement_id": None, "status": "ERROR", "error": "Settlement failed"}

    def close(self):
        pass


def get_settlement_service():
    return SettlementService()
