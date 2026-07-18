from setuptools import setup, find_packages

setup(
    name="fintech-payment-system",
    version="0.1.0",
    description="Production-grade fintech payment processing system",
    author="Anupriya Singh",
    author_email="anewpriya@gmail.com",
    python_requires=">=3.10",
    packages=find_packages(),
    install_requires=[
        "fastapi==0.104.1",
        "uvicorn==0.24.0",
        "pydantic==2.5.0",
        "sqlalchemy==2.0.23",
        "psycopg2-binary==2.9.9",
        "alembic==1.12.1",
        "kafka-python==2.0.2",
        "redis==5.0.0",
        "lightgbm==4.1.0",
        "numpy==1.24.3",
        "pandas==1.5.3",
        "pytest==7.4.3",
        "pytest-asyncio==0.21.1",
        "prometheus-client==0.18.0",
        "python-dotenv==1.0.0",
        "pydantic-settings==2.1.0",
    ],
    entry_points={
        "console_scripts": [
            "fintech-api=src.api.main:app",
        ],
    },
)