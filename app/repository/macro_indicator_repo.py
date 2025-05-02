from datetime import datetime, date
from typing import List, Optional
from sqlalchemy.orm import Session

from repository.model.macro_indicator import MacroIndicator
from repository.database_base import get_db_session

class MacroIndicatorRepository:
    """
    Repository class for handling CRUD operations on MacroIndicator model.
    """
    
    def __init__(self, session: Optional[Session] = None):
        self.session = session or get_db_session()
    
    def create(self, 
               type: str, 
               name: str, 
               value: float, 
               date_time: datetime,
               is_leading_indicator: bool = False,
               region: str = "US") -> MacroIndicator:
        """
        Create a new macro indicator record.
        
        Args:
            type: The type of indicator
            name: The name of the indicator
            value: The indicator value
            date_time: The datetime of the indicator value
            is_leading_indicator: Whether this is a leading indicator
            region: The region of the indicator
            
        Returns:
            The created MacroIndicator instance
        """
        indicator = MacroIndicator(
            type=type,
            name=name,
            value=value,
            date_time=date_time,
            is_leading_indicator=is_leading_indicator,
            region=region,
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
        return self.session.query(MacroIndicator).filter(MacroIndicator.name == name).all()
    
    def find_by_type_and_date(self, indicator_type: str, date_time: date) -> Optional[MacroIndicator]:
        """
        Find a macro indicator by its type and date.
        
        Args:
            indicator_type: The type of the indicator.
            date_time: The date of the indicator value (datetime.date object).
            
        Returns:
            The MacroIndicator instance or None if not found.
        """
        # Ensure date comparison is done only on the date part if date_time is datetime
        start_of_day = datetime.combine(date_time, datetime.min.time())
        end_of_day = datetime.combine(date_time, datetime.max.time())

        return self.session.query(MacroIndicator)\
            .filter(MacroIndicator.type == indicator_type)\
            .filter(MacroIndicator.date_time >= start_of_day)\
            .filter(MacroIndicator.date_time <= end_of_day)\
            .first()
    
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
    
    def delete(self, indicator: MacroIndicator) -> bool:
        """
        Delete a specific macro indicator record.
        
        Args:
            indicator: The MacroIndicator instance to delete.
            
        Returns:
            True if deleted successfully, raises exception otherwise.
        """
        try:
            self.session.delete(indicator)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            # Optionally log the error here
            print(f"Error deleting indicator: {e}") # Replace with logger if available
            raise # Re-raise the exception to signal failure
        
    def close(self):
        """Close the database session."""
        self.session.close() 