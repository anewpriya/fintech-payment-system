"""
Fraud detection rules engine (Tier 1).

Simple, fast rule-based checks that catch obvious fraud in <1ms.
Returns: True (fraud), False (not fraud), or None (uncertain, escalate to Tier 2).
"""

from typing import Optional
from datetime import datetime, timedelta


class FraudRulesEngine:
    """
    Rule-based fraud detection (Tier 1).
    
    Catches obvious fraud quickly:
    - Stolen cards
    - Sanctioned countries
    - Impossible travel
    - Extreme amounts
    """
    
    # In production, these would come from a database or external service
    STOLEN_CARDS = set()  # Placeholder: would be populated from DB
    SANCTIONED_COUNTRIES = {"KP", "IR", "SY", "CU"}  # North Korea, Iran, Syria, Cuba
    MAX_SAFE_AMOUNT = 50000  # Transactions above this are flagged
    
    def __init__(self):
        """Initialize rules engine."""
        self.last_transaction_times = {}  # Track last transaction time per user
        self.last_transaction_countries = {}  # Track last transaction country per user
    
    def check_all_rules(
        self,
        user_id: int,
        card_hash: str,
        amount: float,
        user_country: str,
        device_fingerprint: str,
    ) -> Optional[bool]:
        """
        Run all Tier 1 rules.
        
        Args:
            user_id: User ID
            card_hash: SHA256 hash of card (never full number)
            amount: Transaction amount
            user_country: User's country (ISO code)
            device_fingerprint: Device identifier
        
        Returns:
            True: Fraud detected (DECLINE)
            False: Definitely not fraud (APPROVE)
            None: Uncertain (escalate to Tier 2 ML)
        """
        
        # Rule 1: Is card stolen?
        if self.is_card_stolen(card_hash):
            return True  # Fraud detected
        
        # Rule 2: Is country sanctioned?
        if self.is_sanctioned_country(user_country):
            return True  # Fraud detected
        
        # Rule 3: Impossible travel detection
        if self.is_impossible_travel(user_id, user_country):
            return True  # Fraud detected
        
        # Rule 4: Extreme amount
        if self.is_extreme_amount(amount):
            return None  # Uncertain, escalate to Tier 2
        
        # All rules passed, but no confirmation of legitimacy
        return None  # Uncertain, escalate to Tier 2
    
    def is_card_stolen(self, card_hash: str) -> bool:
        """
        Check if card is in stolen cards list.
        
        In production, this would query a database of reported stolen cards.
        """
        return card_hash in self.STOLEN_CARDS
    
    def is_sanctioned_country(self, country_code: str) -> bool:
        """
        Check if transaction is from sanctioned country.
        
        Sanctions change frequently - in production, query external service.
        """
        return country_code in self.SANCTIONED_COUNTRIES
    
    def is_impossible_travel(self, user_id: int, current_country: str) -> bool:
        """
        Detect if user traveled impossibly fast.
        
        Example: User in NYC at 2:00 PM, then Paris at 2:10 PM
        Minimum flight time NYC-Paris = 6 hours
        This is impossible → fraud
        
        Args:
            user_id: User to check
            current_country: Country of current transaction
        
        Returns:
            True if impossible travel detected
        """
        
        # Get last transaction for this user
        if user_id not in self.last_transaction_times:
            # First transaction for this user
            self.last_transaction_times[user_id] = datetime.utcnow()
            self.last_transaction_countries[user_id] = current_country
            return False  # Can't be impossible travel on first transaction
        
        last_time = self.last_transaction_times[user_id]
        last_country = self.last_transaction_countries[user_id]
        current_time = datetime.utcnow()
        
        # Time elapsed since last transaction
        time_elapsed = current_time - last_time
        
        # If more than 6 hours elapsed, travel is possible
        if time_elapsed > timedelta(hours=6):
            self.last_transaction_times[user_id] = current_time
            self.last_transaction_countries[user_id] = current_country
            return False
        
        # Less than 6 hours elapsed
        # If in different country, this is impossible travel
        if last_country != current_country:
            return True  # Fraud detected
        
        # Same country, less than 6 hours = OK
        self.last_transaction_times[user_id] = current_time
        return False
    
    def is_extreme_amount(self, amount: float) -> bool:
        """
        Check if transaction amount is suspiciously high.
        
        Not definitive fraud (person might legitimately buy expensive item),
        but worth escalating to ML for more analysis.
        """
        return amount > self.MAX_SAFE_AMOUNT