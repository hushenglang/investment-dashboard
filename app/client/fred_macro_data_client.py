import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv

from config.logging_config import get_logger

logger = get_logger(__name__)

class FredMacroDataClient:
    """Service for fetching macroeconomic data from FRED."""
    
    def __init__(self):
        """Initialize the FredMacroDataClient with FRED API credentials."""
        load_dotenv()
        self.fred_api_key = os.getenv('FRED_API_KEY')
        if not self.fred_api_key:
            raise ValueError("FRED API key not found. Please set FRED_API_KEY in .env file")
        self.fred = Fred(api_key=self.fred_api_key)
        logger.info("FredMacroDataClient initialized with FRED API key")


    def _fetch_fred_series(
        self,
        series_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.Series:
        """
        Helper method to fetch a single series from FRED.

        Args:
            series_id (str): The ID of the FRED series to fetch.
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            pd.Series: The fetched data series, indexed by date.

        Raises:
            Exception: If there is an error fetching data from FRED.
        """
        try:
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
            if data.empty:
                logger.warning("No data returned for series %s within the specified date range.", series_id)
            return data
        except Exception as e:
            logger.error("Error fetching FRED series %s: %s", series_id, str(e))
            # Re-raise the exception to be handled by the calling method
            raise Exception(f"Error fetching FRED series {series_id}: {str(e)}")

    def _get_fred_series_value(
        self,
        series_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[pd.Series]:
        """
        Fetches a FRED series and returns all available non-null values.

        Args:
            series_id (str): The ID of the FRED series to fetch.
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Optional[pd.Series]: The series data with all non-null values, or None if no data is available
                             or an error occurred.
        """
        try:
            data = self._fetch_fred_series(series_id, start_date, end_date)
            # Drop any NaN values from the series
            result = data.dropna()
            if result.empty:
                logger.warning("No non-null data available for series %s", series_id)
                return None
            logger.info("Successfully processed %d values for series %s", len(result), series_id)
            return result
        except Exception as e:
            # Error already logged in _fetch_fred_series or potentially here if processing fails
            logger.error("Error processing values for series %s: %s", series_id, str(e))
            return None # Return None on error

    def get_leading_indicators(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch all values for the US Leading Index (USALOLITOAASTSAM)
        and Bundesbank Leading Index (BBKMLEIX) from FRED within the specified date range.

        Args:
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Dict[str, Optional[pd.Series]]: A dictionary containing all available values
                                          for 'leading_index_us' and 'leading_index_de'.
                                          Returns None for a series if data is unavailable or an error occurs.
        """
        series_map = {
            'leading_index_us': 'USALOLITOAASTSAM',
            'leading_index_de': 'BBKMLEIX'
        }
        values: Dict[str, Optional[pd.Series]] = {}
        fetch_errors: List[str] = []

        logger.info("Fetching leading indicator values...")
        for key, series_id in series_map.items():
            value = self._get_fred_series_value(series_id, start_date, end_date)
            if value is not None:
                values[key] = value if not value.empty else None
            else:
                # Logged within the helper, but track errors here
                fetch_errors.append(key)

        if fetch_errors:
             logger.warning("Failed to fetch/process data for indicators: %s", ", ".join(fetch_errors))
             # Continue returning partial data

        logger.info("Fetched leading indicators with %d series", len(values))
        return values

    def get_treasury_yields_and_spreads(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch all Treasury yields (DGS3MO, DGS2, DGS10) and calculate key spreads for the entire period.

        Args:
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Dict[str, Optional[pd.Series]]: A dictionary containing all available yields
                                         and the calculated spreads (10Y-2Y, 10Y-3M) as Series.
                                         Returns None for a series if data is unavailable.
        """
        series_ids = ['DGS3MO', 'DGS2', 'DGS10']
        series_data = {}
        fetch_errors = []

        for series_id in series_ids:
            # Use the dedicated helper for fetching all values
            value = self._get_fred_series_value(series_id, start_date, end_date)
            if value is not None:
                key = series_id.lower()
                series_data[key] = value if not value.empty else None
            else:
                fetch_errors.append(series_id)

        if fetch_errors:
            logger.warning("Failed to fetch/process data for treasury series: %s", ", ".join(fetch_errors))

        # Get the series
        dgs10_series = series_data.get('dgs10')
        dgs2_series = series_data.get('dgs2')
        dgs3mo_series = series_data.get('dgs3mo')

        # Calculate spreads for all dates
        spread_10y_2y = None
        spread_10y_3m = None

        if dgs10_series is not None and dgs2_series is not None:
            # Align the series on their index and calculate the spread
            aligned_10y, aligned_2y = dgs10_series.align(dgs2_series)
            spread_10y_2y = aligned_10y - aligned_2y

        if dgs10_series is not None and dgs3mo_series is not None:
            # Align the series on their index and calculate the spread
            aligned_10y, aligned_3m = dgs10_series.align(dgs3mo_series)
            spread_10y_3m = aligned_10y - aligned_3m

        result = {
            'dgs3mo': dgs3mo_series,
            'dgs2': dgs2_series,
            'dgs10': dgs10_series,
            'spread_10y_2y': spread_10y_2y,
            'spread_10y_3m': spread_10y_3m
        }

        logger.info("Retrieved Treasury yields and calculated spreads for period %s to %s", 
                   start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        return result

    def get_consumer_indices(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch all values for consumer-related indicators from FRED within the specified date range:
        - TOTALSL: Total Consumer Credit
        - UMCSENT: University of Michigan Consumer Sentiment
        - DSPIC96: Real Disposable Personal Income

        Args:
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Dict[str, Optional[pd.Series]]: A dictionary containing all available values
                                          for consumer-related indicators.
                                          Returns None for a series if data is unavailable or an error occurs.
        """
        series_map = {
            'consumer_credit': 'TOTALSL',
            'consumer_sentiment': 'UMCSENT',
            'disposable_income': 'DSPIC96'
        }
        values: Dict[str, Optional[pd.Series]] = {}
        fetch_errors: List[str] = []

        logger.info("Fetching consumer indicator values...")
        for key, series_id in series_map.items():
            value = self._get_fred_series_value(series_id, start_date, end_date)
            if value is not None:
                values[key] = value if not value.empty else None
            else:
                fetch_errors.append(key)

        if fetch_errors:
            logger.warning("Failed to fetch/process data for consumer indicators: %s", ", ".join(fetch_errors))
            # Continue returning partial data

        logger.info("Fetched consumer indices with %d series", len(values))
        return values

    def get_financial_condition_indices(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch all values for financial condition indicators from FRED within the specified date range:
        - NFCI: Chicago Fed National Financial Conditions Index
        - ANFCI: Chicago Fed Adjusted National Financial Conditions Index

        Args:
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Dict[str, Optional[pd.Series]]: A dictionary containing all available values
                                          for financial condition indicators.
                                          Returns None for a series if data is unavailable or an error occurs.
        """
        series_map = {
            'national_financial_conditions': 'NFCI',
            'adjusted_financial_conditions': 'ANFCI'
        }
        values: Dict[str, Optional[pd.Series]] = {}
        fetch_errors: List[str] = []

        logger.info("Fetching financial condition indicator values...")
        for key, series_id in series_map.items():
            value = self._get_fred_series_value(series_id, start_date, end_date)
            if value is not None:
                values[key] = value if not value.empty else None
            else:
                fetch_errors.append(key)

        if fetch_errors:
            logger.warning("Failed to fetch/process data for financial condition indicators: %s", ", ".join(fetch_errors))
            # Continue returning partial data

        logger.info("Fetched financial condition indices with %d series", len(values))
        return values
