import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv

from config.logging_config import get_logger

logger = get_logger(__name__)

class MacroDataService:
    """Service for fetching macroeconomic data from FRED."""
    
    def __init__(self):
        """Initialize the MacroDataService with FRED API credentials."""
        load_dotenv()
        self.fred_api_key = os.getenv('FRED_API_KEY')
        if not self.fred_api_key:
            raise ValueError("FRED API key not found. Please set FRED_API_KEY in .env file")
        self.fred = Fred(api_key=self.fred_api_key)
        logger.info("MacroDataService initialized with FRED API key")

    def _get_date_range(self) -> tuple[datetime, datetime]:
        """
        Get the date range for the last 6 months.
        Returns:
            tuple: (start_date, end_date)
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)  # Approximately 6 months
        return start_date, end_date

    def get_leading_index(self) -> pd.DataFrame:
        """
        Fetch the Leading Index for the United States (USALOLITOAASTSAM) from FRED.
        Returns:
            pd.DataFrame: DataFrame containing the leading index data for the last 6 months
        """
        try:
            start_date, end_date = self._get_date_range()
            series_id = 'USALOLITOAASTSAM'
            
            logger.info(
                "Fetching FRED series: %s with parameters: start_date=%s, end_date=%s",
                series_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
            )
            
            data = self.fred.get_series(
                series_id,
                observation_start=start_date,
                observation_end=end_date
            )
            
            logger.info("Successfully fetched %d observations for series %s", len(data), series_id)
            
            return pd.DataFrame({
                'date': data.index,
                'leading_index': data.values
            })
        except Exception as e:
            logger.error("Error fetching Leading Index data: %s", str(e))
            raise Exception(f"Error fetching Leading Index data: {str(e)}")

    def get_bbk_index(self) -> pd.DataFrame:
        """
        Fetch the Bundesbank Leading Index (BBKMLEIX) from FRED.
        Returns:
            pd.DataFrame: DataFrame containing the BBK index data for the last 6 months
        """
        try:
            start_date, end_date = self._get_date_range()
            series_id = 'BBKMLEIX'
            
            logger.info(
                "Fetching FRED series: %s with parameters: start_date=%s, end_date=%s",
                series_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
            )
            
            data = self.fred.get_series(
                series_id,
                observation_start=start_date,
                observation_end=end_date
            )
            
            logger.info("Successfully fetched %d observations for series %s", len(data), series_id)
            
            return pd.DataFrame({
                'date': data.index,
                'bbk_index': data.values
            })
        except Exception as e:
            logger.error("Error fetching BBK Leading Index data: %s", str(e))
            raise Exception(f"Error fetching BBK Leading Index data: {str(e)}")

    def get_combined_indicators(self) -> pd.DataFrame:
        """
        Fetch and combine both indicators into a single DataFrame.
        Returns:
            pd.DataFrame: DataFrame containing both indicators for the last 6 months
        """
        logger.info("Fetching and combining indicators")
        leading_index = self.get_leading_index()
        bbk_index = self.get_bbk_index()
        
        # Merge the two dataframes on date
        combined_df = pd.merge(
            leading_index,
            bbk_index,
            on='date',
            how='outer'
        )
        
        # Sort by date
        combined_df = combined_df.sort_values('date')
        
        logger.info("Successfully combined indicators, final shape: %s", str(combined_df.shape))
        return combined_df
