from datetime import datetime
from typing import List, Optional
from sqlalchemy.orm import Session

from src.repo.models import MacroIndicator
from src.repo.database import get_db_session

class MacroIndicatorRepository:
    """
    Repository class for handling CRUD operations on MacroIndicator model.
    """
    
    def __init__(self, session: Optional[Session] = None):
        self.session = session or get_db_session()
    
    def create(self, 
               type: str, 
               indicator_name: str, 
               value: float, 
               date_time: datetime,
               is_leading_indicator: bool = False) -> MacroIndicator:
        """
        Create a new macro indicator record.
        
        Args:
            type: The type of indicator
            indicator_name: The name of the indicator
            value: The indicator value
            date_time: The datetime of the indicator value
            is_leading_indicator: Whether this is a leading indicator
            
        Returns:
            The created MacroIndicator instance
        """
        indicator = MacroIndicator(
            type=type,
            indicator_name=indicator_name,
            value=value,
            date_time=date_time,
            is_leading_indicator=is_leading_indicator,
            creation_data_time=datetime.utcnow()
        )
        
        self.session.add(indicator)
        self.session.commit()
        self.session.refresh(indicator)
        
        return indicator
    
    def get_by_id(self, indicator_id: int) -> Optional[MacroIndicator]:
        """
        Get a macro indicator by ID.
        
        Args:
            indicator_id: The ID of the indicator
            
        Returns:
            The MacroIndicator instance or None if not found
        """
        return self.session.query(MacroIndicator).filter(MacroIndicator.id == indicator_id).first()
    
    def get_all(self) -> List[MacroIndicator]:
        """
        Get all macro indicators.
        
        Returns:
            List of all MacroIndicator instances
        """
        return self.session.query(MacroIndicator).all()
    
    def get_by_type(self, indicator_type: str) -> List[MacroIndicator]:
        """
        Get all macro indicators of a specific type.
        
        Args:
            indicator_type: The type of indicators to retrieve
            
        Returns:
            List of matching MacroIndicator instances
        """
        return self.session.query(MacroIndicator).filter(MacroIndicator.type == indicator_type).all()
    
    def get_by_name(self, name: str) -> List[MacroIndicator]:
        """
        Get all macro indicators with a specific name.
        
        Args:
            name: The name of indicators to retrieve
            
        Returns:
            List of matching MacroIndicator instances
        """
        return self.session.query(MacroIndicator).filter(MacroIndicator.indicator_name == name).all()
    
    def update(self, indicator_id: int, **kwargs) -> Optional[MacroIndicator]:
        """
        Update a macro indicator by ID.
        
        Args:
            indicator_id: The ID of the indicator to update
            **kwargs: Fields to update
            
        Returns:
            The updated MacroIndicator instance or None if not found
        """
        indicator = self.get_by_id(indicator_id)
        if indicator:
            for key, value in kwargs.items():
                if hasattr(indicator, key):
                    setattr(indicator, key, value)
            
            self.session.commit()
            self.session.refresh(indicator)
        
        return indicator
    
    def delete(self, indicator_id: int) -> bool:
        """
        Delete a macro indicator by ID.
        
        Args:
            indicator_id: The ID of the indicator to delete
            
        Returns:
            True if deleted, False if not found
        """
        indicator = self.get_by_id(indicator_id)
        if indicator:
            self.session.delete(indicator)
            self.session.commit()
            return True
        
        return False
        
    def close(self):
        """Close the database session."""
        self.session.close() 