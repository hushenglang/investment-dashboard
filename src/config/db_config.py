import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_USER = os.getenv("DB_USER", "hushenglang")
DB_PASSWORD = os.getenv("DB_PASSWORD", "hushenglang")
DB_HOST = os.getenv("DB_HOST", "mysql-container.orb.local")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "investment_dashboard")

# SQLAlchemy database URL with PyMySQL
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}" 