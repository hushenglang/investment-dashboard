import os
from datetime import datetime, timedelta
from typing import Dict, Optional

import yfinance as yf
import pandas as pd
from dotenv import load_dotenv

from config.logging_config import get_logger

logger = get_logger(__name__)

class YahooFinanceClient:
    """Service for fetching commodity price data from Yahoo Finance."""
    
    # Yahoo Finance symbols for commodities
    SYMBOLS = {
        'crude_oil': 'CL=F',  # Crude Oil Futures
        'gold': 'GC=F',       # Gold Futures
    }
    
    def __init__(self):
        """Initialize the YahooFinanceClient."""
        logger.info("YahooFinanceClient initialized")

    def _fetch_commodity_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Helper method to fetch commodity data from Yahoo Finance.

        Args:
            symbol (str): The Yahoo Finance symbol to fetch.
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            pd.DataFrame: The fetched data as a DataFrame.

        Raises:
            Exception: If there is an error fetching data from Yahoo Finance.
        """
        try:
            logger.info(
                "Fetching Yahoo Finance data for %s with parameters: start_date=%s, end_date=%s",
                symbol, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
            )
            
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d')
            )
            
            if df.empty:
                logger.warning("No data returned for %s within the specified date range.", symbol)
                return pd.DataFrame()
            
            logger.info("Successfully fetched %d observations for %s", len(df), symbol)
            return df
            
        except Exception as e:
            logger.error("Error fetching Yahoo Finance data for %s: %s", symbol, str(e))
            raise Exception(f"Error fetching Yahoo Finance data for {symbol}: {str(e)}")

    def _get_latest_price(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[float]:
        """
        Fetches commodity data and returns the latest available closing price.

        Args:
            symbol (str): The Yahoo Finance symbol to fetch.
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Optional[float]: The latest closing price, or None if no data is available
                           or an error occurred.
        """
        try:
            data = self._fetch_commodity_data(symbol, start_date, end_date)
            if data.empty:
                return None
                
            # Get the last available closing price
            last_price = data['Close'].iloc[-1] if not data.empty else None
            result = float(last_price) if last_price is not None else None
            
            logger.info("Successfully processed latest price for %s: %s", symbol, result)
            return result
                
        except Exception as e:
            logger.error("Error processing latest price for %s: %s", symbol, str(e))
            return None

    def get_commodity_prices(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[float]]:
        """
        Fetch the latest prices for crude oil and gold from Yahoo Finance.

        Args:
            start_date (datetime): The start date for the observation period.
            end_date (datetime): The end date for the observation period.

        Returns:
            Dict[str, Optional[float]]: A dictionary containing the latest available prices
                                      for crude oil and gold.
                                      Returns None for a value if data is unavailable or an error occurs.
        """
        latest_prices: Dict[str, Optional[float]] = {}
        fetch_errors: list[str] = []

        logger.info("Fetching latest commodity prices...")
        for commodity, symbol in self.SYMBOLS.items():
            price = self._get_latest_price(symbol, start_date, end_date)
            latest_prices[commodity] = price
            if price is None:
                fetch_errors.append(commodity)

        if fetch_errors:
            logger.warning("Failed to fetch/process latest data for commodities: %s", ", ".join(fetch_errors))

        logger.info("Fetched latest commodity prices: %s", latest_prices)
        return latest_prices 
