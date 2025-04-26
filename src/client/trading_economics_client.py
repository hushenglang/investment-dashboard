import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

from config.logging_config import get_logger

logger = get_logger(__name__)

class TradingEconomicsClient:
    """Service for fetching economic data from Trading Economics API."""
    
    def __init__(self):
        """Initialize the TradingEconomicsClient with Trading Economics API credentials."""
        load_dotenv()
        self.api_key = os.getenv('TRADING_ECONOMICS_API_KEY')
        if not self.api_key:
            raise ValueError("Trading Economics API key not found. Please set TRADING_ECONOMICS_API_KEY in .env file")
        self.base_url = "https://api.tradingeconomics.com/historical"
        logger.info("TradingEconomicsClient initialized with Trading Economics API key")

    def _fetch_indicator(
        self, 
        country: str, 
        indicator: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Helper method to fetch a single indicator from Trading Economics.

        Args:
            country (str): The country name (e.g., 'United States').
            indicator (str): The indicator name to fetch.
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            pd.DataFrame: The fetched data as a DataFrame.

        Raises:
            Exception: If there is an error fetching data from Trading Economics.
        """
        try:
            logger.info(
                "Fetching Trading Economics data: %s for %s with parameters: start_date=%s, end_date=%s",
                indicator, country, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
            )
            
            url = f"{self.base_url}/{country}/{indicator}"
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Client {self.api_key}'
            }
            params = {
                'd1': start_date.strftime('%Y-%m-%d'),
                'd2': end_date.strftime('%Y-%m-%d')
            }
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            data = response.json()
            if not data:
                logger.warning("No data returned for %s %s within the specified date range.", country, indicator)
                return pd.DataFrame()
                
            df = pd.DataFrame(data)
            # Convert dates and set as index
            df['DateTime'] = pd.to_datetime(df['DateTime'])
            df = df.set_index('DateTime')
            
            logger.info("Successfully fetched %d observations for %s %s", len(df), country, indicator)
            return df
            
        except Exception as e:
            logger.error("Error fetching Trading Economics data for %s %s: %s", country, indicator, str(e))
            # Re-raise the exception to be handled by the calling method
            raise Exception(f"Error fetching Trading Economics data for {country} {indicator}: {str(e)}")

    def _get_latest_indicator_value(
        self,
        country: str,
        indicator: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[float]:
        """
        Fetches an indicator and returns the latest available non-null value.

        Args:
            country (str): The country name (e.g., 'United States').
            indicator (str): The indicator name to fetch.
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Optional[float]: The latest non-null value, or None if no data is available
                             or an error occurred.
        """
        try:
            data = self._fetch_indicator(country, indicator, start_date, end_date)
            if data.empty:
                return None
                
            # Get the 'Value' column and the last available non-NaN value
            if 'Value' in data.columns:
                last_value = data['Value'].dropna().iloc[-1] if not data['Value'].dropna().empty else None
                result = float(last_value) if last_value is not None else None
                logger.info("Successfully processed latest value for %s %s: %s", country, indicator, result)
                return result
            else:
                logger.warning("'Value' column not found in response for %s %s", country, indicator)
                return None
                
        except Exception as e:
            logger.error("Error processing latest value for %s %s: %s", country, indicator, str(e))
            return None  # Return None on error

    def get_pmi_indicators(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[float]]:
        """
        Fetch the latest values for PMI indicators from Trading Economics:
        - United States ISM Manufacturing PMI
        - United States ISM Services PMI
        - United States Composite PMI

        Args:
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Dict[str, Optional[float]]: A dictionary containing the latest available values
                                         for PMI indicators.
                                         Returns None for a value if data is unavailable or an error occurs.
        """
        indicators_map = {
            'manufacturing_pmi': 'ISM Manufacturing PMI',
            'services_pmi': 'ISM Services PMI',
            'composite_pmi': 'Composite PMI'
        }
        country = 'United States'
        latest_values: Dict[str, Optional[float]] = {}
        fetch_errors: List[str] = []

        logger.info("Fetching latest PMI indicator values...")
        for key, indicator in indicators_map.items():
            value = self._get_latest_indicator_value(country, indicator, start_date, end_date)
            latest_values[key] = value
            if value is None:
                fetch_errors.append(key)

        if fetch_errors:
            logger.warning("Failed to fetch/process latest data for PMI indicators: %s", ", ".join(fetch_errors))
            # Continue returning partial data

        logger.info("Fetched latest PMI indices: %s", latest_values)
        return latest_values 