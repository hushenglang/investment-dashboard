import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional

from client.fred_macro_data_client import FredMacroDataClient
from repo.macro_indicator_repo import MacroIndicatorRepository
from config.logging_config import get_logger

logger = get_logger(__name__)

class MacroDataService:
    """Service for fetching and storing macroeconomic data."""
    
    def __init__(self):
        """Initialize the MacroDataService with FRED client and repository."""
        self.fred_client = FredMacroDataClient()
        self.repo = MacroIndicatorRepository()
        logger.info("MacroDataService initialized")
    

    #todo: add fetch and store treasury yields and spreads  
    
    def fetch_and_store_leading_indicators(self):
        """
        Fetch indicators from FRED and store them in the database.
        """
        try:
            logger.info("Fetching indicators from FRED")
            start_date = datetime.now() - timedelta(days=180)
            end_date = datetime.now()
            combined_data = self.fred_client.get_leading_indicators(start_date, end_date)
            self._save_leading_indicators_to_db(combined_data)
            logger.info("Successfully fetched and stored indicators")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_indicators: %s", str(e))
            raise
    
    def _save_leading_indicators_to_db(self, data: pd.DataFrame):
        """
        Save the fetched indicators to the database.
        
        Args:
            data: DataFrame containing the indicators data
        """
        try:
            saved_count = 0
            for _, row in data.iterrows():
                date = row['date']
                
                # Process Leading Index
                if 'leading_index' in row and not pd.isna(row['leading_index']):
                    indicator_type = "USALOLITOAASTSAM"
                    indicator_name = "US_LEADING_INDEX"
                    value = float(row['leading_index'])
                    
                    # Check and delete existing record
                    existing_record = self.repo.find_by_type_and_date(indicator_type, date)
                    if existing_record:
                        self.repo.delete(existing_record)
                        logger.debug("Deleted existing %s record for date %s", indicator_name, date.strftime('%Y-%m-%d'))
                    
                    # Save new record
                    self.repo.create(
                        type=indicator_type,
                        indicator_name=indicator_name,
                        value=value,
                        date_time=date,
                        is_leading_indicator=True
                    )
                    saved_count += 1
                
                # Process BBK Index
                if 'bbk_index' in row and not pd.isna(row['bbk_index']):
                    indicator_type = "BBKMLEIX"
                    indicator_name = "BBK_LEADING_INDEX"
                    value = float(row['bbk_index'])

                    # Check and delete existing record
                    existing_record = self.repo.find_by_type_and_date(indicator_type, date)
                    if existing_record:
                        self.repo.delete(existing_record)
                        logger.debug("Deleted existing %s record for date %s", indicator_name, date.strftime('%Y-%m-%d'))

                    # Save new record
                    self.repo.create(
                        type=indicator_type,
                        indicator_name=indicator_name,
                        value=value,
                        date_time=date,
                        is_leading_indicator=True
                    )
                    saved_count += 1
            
            logger.info("Saved %d indicator records to database", saved_count)
        except Exception as e:
            logger.error("Error saving indicators to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()
    
    def get_latest_indicators(self):
        """
        Retrieve the latest indicators from the database.
        
        Returns:
            Dictionary containing the latest indicator values
        """
        try:
            us_indicators = self.repo.get_by_name("US_LEADING_INDEX")
            bbk_indicators = self.repo.get_by_name("BBK_LEADING_INDEX")
            
            latest_data = {
                "US_LEADING_INDEX": max(us_indicators, key=lambda x: x.date_time).value if us_indicators else None,
                "BBK_LEADING_INDEX": max(bbk_indicators, key=lambda x: x.date_time).value if bbk_indicators else None
            }
            
            return latest_data
        except Exception as e:
            logger.error("Error retrieving latest indicators: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_treasury_data(self):
        """
        Fetch treasury yields and spreads from FRED and store them in the database.
        """
        try:
            logger.info("Fetching treasury data from FRED")
            start_date = datetime.now() - timedelta(days=180)
            end_date = datetime.now()
            treasury_data = self.fred_client.get_treasury_yields_and_spreads(start_date, end_date)
            self._save_treasury_data_to_db(treasury_data)
            logger.info("Successfully fetched and stored treasury data")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_treasury_data: %s", str(e))
            raise
    
    def _save_treasury_data_to_db(self, data: Dict[str, Optional[float]]):
        """
        Save the fetched treasury data to the database.
        
        Args:
            data: Dictionary containing the treasury yields and spreads
        """
        try:
            saved_count = 0
            current_date = datetime.now()
            
            # Map of data keys to database indicator types and names
            treasury_indicators = {
                'dgs3mo': ('DGS3MO', 'TREASURY_3M_YIELD'),
                'dgs2': ('DGS2', 'TREASURY_2Y_YIELD'),
                'dgs10': ('DGS10', 'TREASURY_10Y_YIELD'),
                'spread_10y_2y': ('SPREAD_10Y_2Y', 'TREASURY_SPREAD_10Y_2Y'),
                'spread_10y_3m': ('SPREAD_10Y_3M', 'TREASURY_SPREAD_10Y_3M')
            }
            
            for data_key, (indicator_type, indicator_name) in treasury_indicators.items():
                value = data.get(data_key)
                if value is not None:
                    # Check and delete existing record
                    existing_record = self.repo.find_by_type_and_date(indicator_type, current_date)
                    if existing_record:
                        self.repo.delete(existing_record)
                        logger.debug("Deleted existing %s record for date %s", indicator_name, current_date.strftime('%Y-%m-%d'))
                    
                    # Save new record
                    self.repo.create(
                        type=indicator_type,
                        indicator_name=indicator_name,
                        value=float(value),
                        date_time=current_date,
                        is_leading_indicator=False
                    )
                    saved_count += 1
            
            logger.info("Saved %d treasury records to database", saved_count)
        except Exception as e:
            logger.error("Error saving treasury data to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()
