from datetime import datetime
import os
import sys

# Add parent directory to path to allow imports from src
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(parent_dir)

from src.repo.database import init_db
from src.repo.macro_indicator_repo import MacroIndicatorRepository


def main():
    """Example of using the MacroIndicatorRepository."""
    # Initialize the database - creates tables if they don't exist
    print("Initializing database...")
    init_db()
    
    # Create a repository instance
    repo = MacroIndicatorRepository()
    
    try:
        # Create some example data
        print("Creating example records...")
        gdp = repo.create(
            type="economic",
            indicator_name="GDP Growth Rate",
            value=2.3,
            date_time=datetime.now(),
            is_leading_indicator=False
        )
        print(f"Created GDP indicator with ID: {gdp.id}")
        
        unemployment = repo.create(
            type="economic",
            indicator_name="Unemployment Rate",
            value=5.1,
            date_time=datetime.now(),
            is_leading_indicator=False
        )
        print(f"Created Unemployment indicator with ID: {unemployment.id}")
        
        consumer_sentiment = repo.create(
            type="sentiment",
            indicator_name="Consumer Confidence Index",
            value=98.5,
            date_time=datetime.now(),
            is_leading_indicator=True
        )
        print(f"Created Consumer Sentiment indicator with ID: {consumer_sentiment.id}")
        
        # Retrieve and display all indicators
        print("\nAll indicators:")
        all_indicators = repo.get_all()
        for indicator in all_indicators:
            print(f"ID: {indicator.id}, Type: {indicator.type}, "
                  f"Name: {indicator.indicator_name}, Value: {indicator.value}, "
                  f"Date: {indicator.date_time}, Leading: {indicator.is_leading_indicator}")
        
        # Retrieve indicators by type
        print("\nEconomic indicators:")
        economic_indicators = repo.get_by_type("economic")
        for indicator in economic_indicators:
            print(f"ID: {indicator.id}, Name: {indicator.indicator_name}, Value: {indicator.value}")
        
        # Update an indicator
        updated_gdp = repo.update(gdp.id, value=2.5)
        print(f"\nUpdated GDP value to: {updated_gdp.value}")
        
        # Delete an indicator
        repo.delete(unemployment.id)
        print(f"\nDeleted unemployment indicator (ID: {unemployment.id})")
        
        # Verify deletion
        remaining_indicators = repo.get_all()
        print("\nRemaining indicators:")
        for indicator in remaining_indicators:
            print(f"ID: {indicator.id}, Type: {indicator.type}, Name: {indicator.indicator_name}")
        
    finally:
        # Always close the session when done
        repo.close()
        print("\nDatabase session closed")


if __name__ == "__main__":
    main() 