import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional

from client.fred_macro_data_client import FredMacroDataClient
from client.trading_economics_client import TradingEconomicsClient
from client.yahoo_finance_client import YahooFinanceClient
from repo.macro_indicator_repo import MacroIndicatorRepository
from config.logging_config import get_logger

logger = get_logger(__name__)

class MacroDataService:
    """Service for fetching and storing macroeconomic data."""
    
    def __init__(self):
        """Initialize the MacroDataService with FRED client and repository."""
        self.fred_client = FredMacroDataClient()
        self.trading_economics_client = TradingEconomicsClient()
        self.yahoo_finance_client = YahooFinanceClient()
        self.repo = MacroIndicatorRepository()
        logger.info("MacroDataService initialized")
    
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

    def fetch_and_store_consumer_indices(self):
        """
        Fetch consumer-related indices from FRED and store them in the database.
        
        This method fetches the latest values for:
        - Consumer credit (TOTALSL)
        - Consumer sentiment (UMCSENT)
        - Disposable income (DSPIC96)
        
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching consumer indices from FRED")
            start_date = datetime.now() - timedelta(days=180)
            end_date = datetime.now()
            consumer_data = self.fred_client.get_consumer_indices(start_date, end_date)
            self._save_consumer_indices_to_db(consumer_data)
            logger.info("Successfully fetched and stored consumer indices")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_consumer_indices: %s", str(e))
            raise
    
    def _save_consumer_indices_to_db(self, data: Dict[str, Optional[float]]):
        """
        Save the fetched consumer indices to the database.
        
        Args:
            data: Dictionary containing the consumer indices
        """
        try:
            saved_count = 0
            current_date = datetime.now()
            
            # Map of data keys to database indicator types and names
            consumer_indicators = {
                'consumer_credit': ('TOTALSL', 'CONSUMER_CREDIT'),
                'consumer_sentiment': ('UMCSENT', 'CONSUMER_SENTIMENT'),
                'disposable_income': ('DSPIC96', 'DISPOSABLE_INCOME')
            }
            
            for data_key, (indicator_type, indicator_name) in consumer_indicators.items():
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
            
            logger.info("Saved %d consumer index records to database", saved_count)
        except Exception as e:
            logger.error("Error saving consumer indices to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()

    def fetch_and_store_financial_condition_indices(self):
        """
        Fetch financial condition indices from FRED and store them in the database.
        
        This method fetches the latest values for:
        - National Financial Conditions Index (NFCI)
        - Adjusted National Financial Conditions Index (ANFCI)
        
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching financial condition indices from FRED")
            start_date = datetime.now() - timedelta(days=180)
            end_date = datetime.now()
            financial_data = self.fred_client.get_financial_condition_indices(start_date, end_date)
            self._save_financial_condition_indices_to_db(financial_data)
            logger.info("Successfully fetched and stored financial condition indices")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_financial_condition_indices: %s", str(e))
            raise
    
    def _save_financial_condition_indices_to_db(self, data: Dict[str, Optional[float]]):
        """
        Save the fetched financial condition indices to the database.
        
        Args:
            data: Dictionary containing the financial condition indices
        """
        try:
            saved_count = 0
            current_date = datetime.now()
            
            # Map of data keys to database indicator types and names
            financial_indicators = {
                'national_financial_conditions': ('NFCI', 'NATIONAL_FINANCIAL_CONDITIONS'),
                'adjusted_financial_conditions': ('ANFCI', 'ADJUSTED_FINANCIAL_CONDITIONS')
            }
            
            for data_key, (indicator_type, indicator_name) in financial_indicators.items():
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
            
            logger.info("Saved %d financial condition index records to database", saved_count)
        except Exception as e:
            logger.error("Error saving financial condition indices to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()

    def fetch_and_store_pmi_indicators(self):
        """
        Fetch PMI indicators from Trading Economics and store them in the database.
        
        This method fetches the latest values for:
        - ISM Manufacturing PMI
        - ISM Services PMI
        - Composite PMI
        
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching PMI indicators from Trading Economics")
            start_date = datetime.now() - timedelta(days=180)
            end_date = datetime.now()
            pmi_data = self.trading_economics_client.get_pmi_indicators(start_date, end_date)
            self._save_pmi_indicators_to_db(pmi_data)
            logger.info("Successfully fetched and stored PMI indicators")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_pmi_indicators: %s", str(e))
            raise
    
    def _save_pmi_indicators_to_db(self, data: Dict[str, Optional[float]]):
        """
        Save the fetched PMI indicators to the database.
        
        Args:
            data: Dictionary containing the PMI indicators
        """
        try:
            saved_count = 0
            current_date = datetime.now()
            
            # Map of data keys to database indicator types and names
            pmi_indicators = {
                'manufacturing_pmi': ('ISM_MAN_PMI', 'MANUFACTURING_PMI'),
                'services_pmi': ('ISM_SERV_PMI', 'SERVICES_PMI'),
                'composite_pmi': ('COMP_PMI', 'COMPOSITE_PMI')
            }
            
            for data_key, (indicator_type, indicator_name) in pmi_indicators.items():
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
            
            logger.info("Saved %d PMI indicator records to database", saved_count)
        except Exception as e:
            logger.error("Error saving PMI indicators to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()

    def fetch_and_store_commodity_prices(self):
        """
        Fetch commodity prices from Yahoo Finance and store them in the database.
        
        This method fetches the latest values for:
        - Crude Oil Futures
        - Gold Futures
        
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching commodity prices from Yahoo Finance")
            start_date = datetime.now() - timedelta(days=7)
            end_date = datetime.now()
            commodity_data = self.yahoo_finance_client.get_commodity_prices(start_date, end_date)
            self._save_commodity_prices_to_db(commodity_data)
            logger.info("Successfully fetched and stored commodity prices")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_commodity_prices: %s", str(e))
            raise
    
    def _save_commodity_prices_to_db(self, data: Dict[str, Optional[float]]):
        """
        Save the fetched commodity prices to the database.
        
        Args:
            data: Dictionary containing the commodity prices
        """
        try:
            saved_count = 0
            current_date = datetime.now()
            
            # Map of data keys to database indicator types and names
            commodity_indicators = {
                'crude_oil': ('CRUDE_OIL_FUT', 'CRUDE_OIL_FUTURES'),
                'gold': ('GOLD_FUT', 'GOLD_FUTURES')
            }
            
            for data_key, (indicator_type, indicator_name) in commodity_indicators.items():
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
            
            logger.info("Saved %d commodity price records to database", saved_count)
        except Exception as e:
            logger.error("Error saving commodity prices to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()

