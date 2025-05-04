import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional
from client.fred_macro_data_client import FredMacroDataClient
from client.yahoo_finance_client import YahooFinanceClient
from repository.macro_indicator_repo import MacroIndicatorRepository
from config.logging_config import get_logger

logger = get_logger(__name__)

class MacroDataService:
    """Service for fetching and storing macroeconomic data."""
    
    def __init__(self):
        """Initialize the MacroDataService with FRED client and repository."""
        self.fred_client = FredMacroDataClient()
        self.yahoo_finance_client = YahooFinanceClient()
        self.repo = MacroIndicatorRepository()
        logger.info("MacroDataService initialized")

    def get_all_us_indicators(self, start_date: datetime, end_date: datetime):
        """
        Retrieve all US indicators within a specified date range from the database.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
        
        Returns:
            Dictionary containing the indicator values where keys are indicator types
            and values are dictionaries with indicator details
        """
        try:
            logger.info("Retrieving US indicators from database for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            indicators = self.repo.find_by_region_date_range(
                start_date=start_date,
                end_date=end_date,
                region="US"
            )
            logger.info("Successfully retrieved %d US indicators", len(indicators))
            indicators_dict = {  
                indicator.type: {
                    "type": indicator.type,
                    "name": indicator.name,
                    "value": indicator.value,
                    "date_time": indicator.date_time,
                    "is_leading_indicator": indicator.is_leading_indicator,
                    "region": indicator.region
                }
                for indicator in indicators
            }
            return indicators_dict
        except Exception as e:
            logger.error("Error retrieving US indicators: %s", str(e))
            raise
        finally:
            self.repo.close()

    def get_all_china_indicators(self, start_date: datetime, end_date: datetime):
        """
        Retrieve all China indicators within a specified date range from the database.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
        
        Returns:
            Dictionary containing the indicator values where keys are indicator types
            and values are dictionaries with indicator details
        """
        try:
            logger.info("Retrieving China indicators from database for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            indicators = self.repo.find_by_region_date_range(
                start_date=start_date,
                end_date=end_date,
                region="CHINA"
            )
            logger.info("Successfully retrieved %d China indicators", len(indicators))
            indicators_dict = {  
                indicator.type: {
                    "type": indicator.type,
                    "name": indicator.name,
                    "value": indicator.value,
                    "date_time": indicator.date_time,
                    "is_leading_indicator": indicator.is_leading_indicator,
                    "region": indicator.region
                }
                for indicator in indicators
            }
            return indicators_dict
        except Exception as e:
            logger.error("Error retrieving China indicators: %s", str(e))
            raise
        finally:
            self.repo.close()
    
    def fetch_and_store_leading_indicators(self, default_days: int = 180):
        """
        Fetch indicators from FRED and store them in the database.
        
        Args:
            default_days (int): Number of days to look back for data (default: 180)
        """
        start_date = datetime.now() - timedelta(days=default_days)
        end_date = datetime.now()
        return self.fetch_and_store_leading_indicators_by_date_range(start_date, end_date)

    def _save_leading_indicators_to_db(self, data: Dict[str, Optional[pd.Series]]):
        """
        Save the fetched indicators to the database.
        
        Args:
            data: Dictionary containing the indicators data where keys are indicator types
                 and values are pandas Series with the indicator values
        """
        try:
            saved_count = 0
            
            # Process US Leading Index
            if 'leading_index_us' in data and data['leading_index_us'] is not None:
                us_data = data['leading_index_us']
                for date, value in us_data.items():
                    if pd.notna(value):  # Check if value is not NaN
                        # Check and delete existing record
                        existing_record = self.repo.find_by_type_and_date("USALOLITOAASTSAM", date)
                        if existing_record:
                            self.repo.delete(existing_record)
                            logger.debug("Deleted existing US_LEADING_INDEX record for date %s", date.strftime('%Y-%m-%d'))
                        
                        # Save new record
                        self.repo.create(
                            type="USALOLITOAASTSAM",
                            name="US_LEADING_INDEX",
                            value=float(value),
                            date_time=date,
                            is_leading_indicator=True,
                            region="US"
                        )
                        saved_count += 1
            
            # Process BBK Index (German Leading Index)
            if 'leading_index_de' in data and data['leading_index_de'] is not None:
                de_data = data['leading_index_de']
                for date, value in de_data.items():
                    if pd.notna(value):  # Check if value is not NaN
                        # Check and delete existing record
                        existing_record = self.repo.find_by_type_and_date("BBKMLEIX", date)
                        if existing_record:
                            self.repo.delete(existing_record)
                            logger.debug("Deleted existing BBK_LEADING_INDEX record for date %s", date.strftime('%Y-%m-%d'))

                        # Save new record
                        self.repo.create(
                            type="BBKMLEIX",
                            name="BBK_LEADING_INDEX",
                            value=float(value),
                            date_time=date,
                            is_leading_indicator=True,
                            region="US"
                        )
                        saved_count += 1
            
            logger.info("Saved %d indicator records to database", saved_count)
        except Exception as e:
            logger.error("Error saving indicators to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()
    
    def fetch_and_store_treasury_data(self, default_days: int = 180):
        """
        Fetch treasury yields and spreads from FRED and store them in the database.
        
        Args:
            default_days (int): Number of days to look back for data (default: 180)
        """
        start_date = datetime.now() - timedelta(days=default_days)
        end_date = datetime.now()
        return self.fetch_and_store_treasury_data_by_date_range(start_date, end_date)

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
                        name=indicator_name,
                        value=float(value),
                        date_time=current_date,
                        is_leading_indicator=False,
                        region="US"
                    )
                    saved_count += 1
            
            logger.info("Saved %d treasury records to database", saved_count)
        except Exception as e:
            logger.error("Error saving treasury data to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()

    def fetch_and_store_consumer_indices(self, default_days: int = 180):
        """
        Fetch consumer-related indices from FRED and store them in the database.
        
        Args:
            default_days (int): Number of days to look back for data (default: 180)
        """
        start_date = datetime.now() - timedelta(days=default_days)
        end_date = datetime.now()
        return self.fetch_and_store_consumer_indices_by_date_range(start_date, end_date)
    
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
                        name=indicator_name,
                        value=float(value),
                        date_time=current_date,
                        is_leading_indicator=False,
                        region="US"
                    )
                    saved_count += 1
            
            logger.info("Saved %d consumer index records to database", saved_count)
        except Exception as e:
            logger.error("Error saving consumer indices to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()

    def fetch_and_store_financial_condition_indices(self, default_days: int = 180):
        """
        Fetch financial condition indices from FRED and store them in the database.
        
        Args:
            default_days (int): Number of days to look back for data (default: 180)
        """
        start_date = datetime.now() - timedelta(days=default_days)
        end_date = datetime.now()
        return self.fetch_and_store_financial_condition_indices_by_date_range(start_date, end_date)
    
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
                        name=indicator_name,
                        value=float(value),
                        date_time=current_date,
                        is_leading_indicator=False,
                        region="US"
                    )
                    saved_count += 1
            
            logger.info("Saved %d financial condition index records to database", saved_count)
        except Exception as e:
            logger.error("Error saving financial condition indices to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()

    def fetch_and_store_pmi_indicators(self, default_days: int = 180):
        """
        Fetch PMI indicators from Trading Economics and store them in the database.
        
        Args:
            default_days (int): Number of days to look back for data (default: 180)
        """
        start_date = datetime.now() - timedelta(days=default_days)
        end_date = datetime.now()
        return self.fetch_and_store_pmi_indicators_by_date_range(start_date, end_date)
    
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
                        name=indicator_name,
                        value=float(value),
                        date_time=current_date,
                        is_leading_indicator=False,
                        region="US"
                    )
                    saved_count += 1
            
            logger.info("Saved %d PMI indicator records to database", saved_count)
        except Exception as e:
            logger.error("Error saving PMI indicators to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()

    def fetch_and_store_commodity_prices(self, default_days: int = 180):
        """
        Fetch commodity prices from Yahoo Finance and store them in the database.
        
        Args:
            default_days (int): Number of days to look back for data (default: 180)
        """
        start_date = datetime.now() - timedelta(days=default_days)
        end_date = datetime.now()
        return self.fetch_and_store_commodity_prices_by_date_range(start_date, end_date)
    
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
                        name=indicator_name,
                        value=float(value),
                        date_time=current_date,
                        is_leading_indicator=False,
                        region="US"
                    )
                    saved_count += 1
            
            logger.info("Saved %d commodity price records to database", saved_count)
        except Exception as e:
            logger.error("Error saving commodity prices to database: %s", str(e))
            self.repo.session.rollback()
            raise
        finally:
            self.repo.close()

    def fetch_and_store_leading_indicators_by_date_range(self, start_date: datetime, end_date: datetime):
        """
        Fetch indicators from FRED and store them in the database for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching indicators from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            combined_data = self.fred_client.get_leading_indicators(start_date, end_date)
            self._save_leading_indicators_to_db(combined_data)
            logger.info("Successfully fetched and stored indicators")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_indicators_by_date_range: %s", str(e))
            raise

    def fetch_and_store_treasury_data_by_date_range(self, start_date: datetime, end_date: datetime):
        """
        Fetch treasury yields and spreads from FRED and store them in the database for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching treasury data from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            treasury_data = self.fred_client.get_treasury_yields_and_spreads(start_date, end_date)
            self._save_treasury_data_to_db(treasury_data)
            logger.info("Successfully fetched and stored treasury data")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_treasury_data_by_date_range: %s", str(e))
            raise

    def fetch_and_store_consumer_indices_by_date_range(self, start_date: datetime, end_date: datetime):
        """
        Fetch consumer-related indices from FRED and store them in the database for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching consumer indices from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            consumer_data = self.fred_client.get_consumer_indices(start_date, end_date)
            self._save_consumer_indices_to_db(consumer_data)
            logger.info("Successfully fetched and stored consumer indices")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_consumer_indices_by_date_range: %s", str(e))
            raise

    def fetch_and_store_financial_condition_indices_by_date_range(self, start_date: datetime, end_date: datetime):
        """
        Fetch financial condition indices from FRED and store them in the database for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching financial condition indices from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            financial_data = self.fred_client.get_financial_condition_indices(start_date, end_date)
            self._save_financial_condition_indices_to_db(financial_data)
            logger.info("Successfully fetched and stored financial condition indices")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_financial_condition_indices_by_date_range: %s", str(e))
            raise

    def fetch_and_store_commodity_prices_by_date_range(self, start_date: datetime, end_date: datetime):
        """
        Fetch commodity prices from Yahoo Finance and store them in the database for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching commodity prices from Yahoo Finance for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            commodity_data = self.yahoo_finance_client.get_commodity_prices(start_date, end_date)
            self._save_commodity_prices_to_db(commodity_data)
            logger.info("Successfully fetched and stored commodity prices")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_commodity_prices_by_date_range: %s", str(e))
            raise

    def fetch_and_store_leading_index(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store US Leading Index for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching US Leading Index from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.fred_client.get_leading_index(start_date, end_date)
            if data is not None and not data.empty:
                for _, row in data.iterrows():
                    date = row['date']
                    value = float(row['leading_index'])
                    
                    # Check and delete existing record
                    existing_record = self.repo.find_by_type_and_date("USALOLITOAASTSAM", date)
                    if existing_record:
                        self.repo.delete(existing_record)
                    
                    # Save new record
                    self.repo.create(
                        type="USALOLITOAASTSAM",
                        name="US_LEADING_INDEX",
                        value=value,
                        date_time=date,
                        is_leading_indicator=True,
                        region="US"
                    )
            logger.info("Successfully fetched and stored US Leading Index")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_leading_index: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_bbk_index(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store BBK Leading Index for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching BBK Leading Index from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.fred_client.get_bbk_index(start_date, end_date)
            if data is not None and not data.empty:
                for _, row in data.iterrows():
                    date = row['date']
                    value = float(row['bbk_index'])
                    
                    # Check and delete existing record
                    existing_record = self.repo.find_by_type_and_date("BBKMLEIX", date)
                    if existing_record:
                        self.repo.delete(existing_record)
                    
                    # Save new record
                    self.repo.create(
                        type="BBKMLEIX",
                        name="BBK_LEADING_INDEX",
                        value=value,
                        date_time=date,
                        is_leading_indicator=True,
                        region="US"
                    )
            logger.info("Successfully fetched and stored BBK Leading Index")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_bbk_index: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_treasury_yield_3m(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store 3-Month Treasury Yield for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching 3-Month Treasury Yield from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.fred_client.get_treasury_yield_3m(start_date, end_date)
            if data is not None:
                # Check and delete existing record
                existing_record = self.repo.find_by_type_and_date("DGS3MO", end_date)
                if existing_record:
                    self.repo.delete(existing_record)
                
                # Save new record
                self.repo.create(
                    type="DGS3MO",
                    name="TREASURY_3M_YIELD",
                    value=float(data),
                    date_time=end_date,
                    is_leading_indicator=False,
                    region="US"
                )
            logger.info("Successfully fetched and stored 3-Month Treasury Yield")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_treasury_yield_3m: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_treasury_yield_2y(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store 2-Year Treasury Yield for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching 2-Year Treasury Yield from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.fred_client.get_treasury_yield_2y(start_date, end_date)
            if data is not None:
                # Check and delete existing record
                existing_record = self.repo.find_by_type_and_date("DGS2", end_date)
                if existing_record:
                    self.repo.delete(existing_record)
                
                # Save new record
                self.repo.create(
                    type="DGS2",
                    name="TREASURY_2Y_YIELD",
                    value=float(data),
                    date_time=end_date,
                    is_leading_indicator=False,
                    region="US"
                )
            logger.info("Successfully fetched and stored 2-Year Treasury Yield")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_treasury_yield_2y: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_treasury_yield_10y(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store 10-Year Treasury Yield for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching 10-Year Treasury Yield from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.fred_client.get_treasury_yield_10y(start_date, end_date)
            if data is not None:
                # Check and delete existing record
                existing_record = self.repo.find_by_type_and_date("DGS10", end_date)
                if existing_record:
                    self.repo.delete(existing_record)
                
                # Save new record
                self.repo.create(
                    type="DGS10",
                    name="TREASURY_10Y_YIELD",
                    value=float(data),
                    date_time=end_date,
                    is_leading_indicator=False,
                    region="US"
                )
            logger.info("Successfully fetched and stored 10-Year Treasury Yield")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_treasury_yield_10y: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_consumer_credit(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store Consumer Credit for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching Consumer Credit from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.fred_client.get_consumer_credit(start_date, end_date)
            if data is not None:
                # Check and delete existing record
                existing_record = self.repo.find_by_type_and_date("TOTALSL", end_date)
                if existing_record:
                    self.repo.delete(existing_record)
                
                # Save new record
                self.repo.create(
                    type="TOTALSL",
                    name="CONSUMER_CREDIT",
                    value=float(data),
                    date_time=end_date,
                    is_leading_indicator=False,
                    region="US"
                )
            logger.info("Successfully fetched and stored Consumer Credit")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_consumer_credit: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_consumer_sentiment(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store Consumer Sentiment for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching Consumer Sentiment from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.fred_client.get_consumer_sentiment(start_date, end_date)
            if data is not None:
                # Check and delete existing record
                existing_record = self.repo.find_by_type_and_date("UMCSENT", end_date)
                if existing_record:
                    self.repo.delete(existing_record)
                
                # Save new record
                self.repo.create(
                    type="UMCSENT",
                    name="CONSUMER_SENTIMENT",
                    value=float(data),
                    date_time=end_date,
                    is_leading_indicator=False,
                    region="US"
                )
            logger.info("Successfully fetched and stored Consumer Sentiment")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_consumer_sentiment: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_disposable_income(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store Disposable Income for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching Disposable Income from FRED for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.fred_client.get_disposable_income(start_date, end_date)
            if data is not None:
                # Check and delete existing record
                existing_record = self.repo.find_by_type_and_date("DSPIC96", end_date)
                if existing_record:
                    self.repo.delete(existing_record)
                
                # Save new record
                self.repo.create(
                    type="DSPIC96",
                    name="DISPOSABLE_INCOME",
                    value=float(data),
                    date_time=end_date,
                    is_leading_indicator=False,
                    region="US"
                )
            logger.info("Successfully fetched and stored Disposable Income")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_disposable_income: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_crude_oil(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store Crude Oil Futures price for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching Crude Oil Futures from Yahoo Finance for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.yahoo_finance_client.get_crude_oil_price(start_date, end_date)
            if data is not None:
                # Check and delete existing record
                existing_record = self.repo.find_by_type_and_date("CRUDE_OIL_FUT", end_date)
                if existing_record:
                    self.repo.delete(existing_record)
                
                # Save new record
                self.repo.create(
                    type="CRUDE_OIL_FUT",
                    name="CRUDE_OIL_FUTURES",
                    value=float(data),
                    date_time=end_date,
                    is_leading_indicator=False,
                    region="US"
                )
            logger.info("Successfully fetched and stored Crude Oil Futures")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_crude_oil: %s", str(e))
            raise
        finally:
            self.repo.close()

    def fetch_and_store_gold(self, start_date: datetime, end_date: datetime):
        """
        Fetch and store Gold Futures price for a specific date range.
        
        Args:
            start_date (datetime): Start date of the range (inclusive)
            end_date (datetime): End date of the range (inclusive)
            
        Returns:
            bool: True if operation was successful
        """
        try:
            logger.info("Fetching Gold Futures from Yahoo Finance for period %s to %s", 
                       start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            data = self.yahoo_finance_client.get_gold_price(start_date, end_date)
            if data is not None:
                # Check and delete existing record
                existing_record = self.repo.find_by_type_and_date("GOLD_FUT", end_date)
                if existing_record:
                    self.repo.delete(existing_record)
                
                # Save new record
                self.repo.create(
                    type="GOLD_FUT",
                    name="GOLD_FUTURES",
                    value=float(data),
                    date_time=end_date,
                    is_leading_indicator=False,
                    region="US"
                )
            logger.info("Successfully fetched and stored Gold Futures")
            return True
        except Exception as e:
            logger.error("Error in fetch_and_store_gold: %s", str(e))
            raise
        finally:
            self.repo.close()

