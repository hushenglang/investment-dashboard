from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class MacroIndicator(Base):
    """
    SQLAlchemy model for the macro_indicator table.
    """
    __tablename__ = 'macro_indicator'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(100), nullable=False)
    indicator_name = Column(String(100), nullable=False)
    value = Column(Float, nullable=False)
    date_time = Column(DateTime, nullable=False)
    is_leading_indicator = Column(Boolean, default=False)
    creation_data_time = Column(DateTime, default=datetime.utcnow) 