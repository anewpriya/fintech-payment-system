"""
Database connection management.

This module handles PostgreSQL connection pooling and session management.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from src.config import settings

# Create database engine
# echo=True logs all SQL queries (useful for debugging)
engine = create_engine(
    settings.database_url,
    pool_size=settings.database_pool_size,
    max_overflow=settings.database_max_overflow,
    echo=settings.debug,  # Log SQL in debug mode
    pool_pre_ping=True,   # Test connection before using (prevents "connection lost" errors)
    pool_recycle=3600,    # Recycle connections every hour
)

# Create session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db() -> Session:
    """
    Dependency injection function for FastAPI.
    
    Returns a database session that can be used in API endpoints.
    Session is automatically closed after request completes.
    
    Usage in FastAPI:
        @app.post("/transactions")
        def create_transaction(db: Session = Depends(get_db)):
            # Use db here
            pass
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """
    Initialize the database (create all tables).
    
    This should be called once on application startup if using
    Alembic migrations. If not using migrations, this creates tables
    from the models.
    """
    from src.models.transaction import Base
    Base.metadata.create_all(bind=engine)


def close_db():
    """
    Close all database connections.
    
    Called on application shutdown.
    """
    engine.dispose()