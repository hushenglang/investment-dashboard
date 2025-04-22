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

    def _get_latest_fred_series_value(
        self,
        series_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[float]:
        """
        Fetches a FRED series and returns the latest available non-null value.

        Args:
            series_id (str): The ID of the FRED series to fetch.
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Optional[float]: The latest non-null value, or None if no data is available
                             or an error occurred.
        """
        try:
            data = self._fetch_fred_series(series_id, start_date, end_date)
            # Get the last available non-NaN value
            last_value = data.dropna().iloc[-1] if not data.dropna().empty else None
            result = float(last_value) if last_value is not None else None
            logger.info("Successfully processed latest value for series %s: %s", series_id, result)
            return result
        except Exception as e:
            # Error already logged in _fetch_fred_series or potentially here if processing fails
            logger.error("Error processing latest value for series %s: %s", series_id, str(e))
            return None # Return None on error

    def get_leading_indicators(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[float]]:
        """
        Fetch the latest values for the US Leading Index (USALOLITOAASTSAM)
        and Bundesbank Leading Index (BBKMLEIX) from FRED.

        Args:
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Dict[str, Optional[float]]: A dictionary containing the latest available values
                                         for 'leading_index_us' and 'leading_index_de'.
                                         Returns None for a value if data is unavailable or an error occurs.
        """
        series_map = {
            'leading_index_us': 'USALOLITOAASTSAM',
            'leading_index_de': 'BBKMLEIX'
        }
        latest_values: Dict[str, Optional[float]] = {}
        fetch_errors: List[str] = []

        logger.info("Fetching latest leading indicator values...")
        for key, series_id in series_map.items():
            value = self._get_latest_fred_series_value(series_id, start_date, end_date)
            latest_values[key] = value
            if value is None:
                # Logged within the helper, but track errors here
                fetch_errors.append(key)

        if fetch_errors:
             logger.warning("Failed to fetch/process latest data for indicators: %s", ", ".join(fetch_errors))
             # Continue returning partial data

        logger.info("Fetched latest leading indicators: %s", latest_values)
        return latest_values

    def get_treasury_yields_and_spreads(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[float]]:
        """
        Fetch the latest Treasury yields (DGS3MO, DGS2, DGS10) and calculate key spreads.

        Args:
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Dict[str, Optional[float]]: A dictionary containing the latest available yields
                                         and the calculated spreads (10Y-2Y, 10Y-3M).
                                         Returns None for a value if data is unavailable.
        """
        series_ids = ['DGS3MO', 'DGS2', 'DGS10']
        latest_data = {}
        fetch_errors = []

        for series_id in series_ids:
            # Use the dedicated helper for fetching the latest value
            value = self._get_latest_fred_series_value(series_id, start_date, end_date)
            key = series_id.lower()
            latest_data[key] = value
            if value is None:
                 fetch_errors.append(series_id)
            # Logging for success/failure happens within the helper

        if fetch_errors:
            logger.warning("Failed to fetch/process latest data for treasury series: %s", ", ".join(fetch_errors))
            # Depending on requirements, we might want to raise an error here or return partial data.
            # Currently returning partial data with None for missing values.

        # Calculate spreads
        dgs10 = latest_data.get('dgs10')
        dgs2 = latest_data.get('dgs2')
        dgs3mo = latest_data.get('dgs3mo')

        spread_10y_2y = (dgs10 - dgs2) if dgs10 is not None and dgs2 is not None else None
        spread_10y_3m = (dgs10 - dgs3mo) if dgs10 is not None and dgs3mo is not None else None

        result = {
            'dgs3mo': dgs3mo,
            'dgs2': dgs2,
            'dgs10': dgs10,
            'spread_10y_2y': spread_10y_2y,
            'spread_10y_3m': spread_10y_3m
        }

        logger.info("Calculated Treasury yields and spreads: %s", result)
        return result
