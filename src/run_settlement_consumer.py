"""
Script to run the settlement consumer.

Usage:
    python -m src.run_settlement_consumer

This starts a long-running process that:
1. Consumes fraud decisions from Kafka
2. Settles approved transactions
3. Publishes settlement results
"""

import logging
import sys

from src.services.settlement_consumer import get_settlement_consumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__)


def main():
    """Run settlement consumer."""
    
    logger.info("=" * 80)
    logger.info("SETTLEMENT SERVICE - STARTING")
    logger.info("=" * 80)
    
    try:
        # Get consumer instance
        consumer = get_settlement_consumer()
        
        # Connect to Kafka
        logger.info("Connecting to Kafka...")
        consumer.connect()
        
        logger.info("=" * 80)
        logger.info("✓ Consumer ready, waiting for fraud decisions...")
        logger.info("=" * 80)
        
        # Start processing
        consumer.run()
    
    except KeyboardInterrupt:
        logger.info("Received shutdown signal (Ctrl+C)")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()