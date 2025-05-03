from datetime import datetime, date
from typing import List, Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func

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
    
    def find_by_region_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        region: str = "US"
    ) -> List[MacroIndicator]:
        """
        Get macro indicators for a specific region within a date range.
        
        Args:
            start_date: The start datetime to filter from (inclusive)
            end_date: The end datetime to filter to (inclusive)
            region: The region to filter indicators by (default: "US")
            
        Returns:
            List[MacroIndicator]: List of macro indicators matching the criteria
        """
        return self.session.query(MacroIndicator)\
            .filter(MacroIndicator.region == region)\
            .filter(MacroIndicator.date_time >= start_date)\
            .filter(MacroIndicator.date_time <= end_date)\
            .order_by(MacroIndicator.date_time).all() 
    
    def close(self):
        """Close the database session."""
        self.session.close()

    