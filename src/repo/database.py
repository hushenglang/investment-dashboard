import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from src.config.db_config import DATABASE_URL
from src.repo.models import Base

# Register PyMySQL with SQLAlchemy
pymysql.install_as_MySQLdb()

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create session factory
session_factory = sessionmaker(bind=engine)
SessionLocal = scoped_session(session_factory)

# Function to initialize the database
def init_db():
    """Initialize the database by creating all tables."""
    Base.metadata.create_all(bind=engine)

# Function to get a database session
def get_db_session():
    """Get a new database session."""
    db = SessionLocal()
    try:
        return db
    finally:
        db.close() 