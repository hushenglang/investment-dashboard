import akshare as ak
from datetime import datetime, timedelta
from typing import Dict, Optional
import pandas as pd
from config.logging_config import get_logger

logger = get_logger(__name__)

class AkshareFinanceClient:
    """Service for fetching China economic indicators from akshare."""
    
    def __init__(self):
        """Initialize the AkshareFinanceClient."""
        logger.info("AkshareFinanceClient initialized")

    def get_gdp_growth(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's quarterly GDP growth rate for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing GDP growth rate time series
        """
        try:
            # Get GDP data
            df = ak.macro_china_gdp()
            if df.empty:
                logger.warning("No GDP data available")
                return {'gdp_yoy': None}
            
            # Convert date column to datetime
            df['季度'] = pd.to_datetime(df['季度'])
            
            # Filter by date range
            mask = (df['季度'] >= start_date) & (df['季度'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No GDP data available for specified date range")
                return {'gdp_yoy': None}
            
            # Create time series with date index
            gdp_series = pd.Series(df_filtered['GDP同比增长'].astype(float).values, index=df_filtered['季度'])
            
            logger.info("Successfully fetched GDP growth rate time series")
            return {'gdp_yoy': gdp_series}
        except Exception as e:
            logger.error("Error fetching GDP growth rate: %s", str(e))
            return {'gdp_yoy': None}

    def get_industrial_production(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's industrial production growth rate for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing industrial production growth rate time series
        """
        try:
            # Get industrial production data
            df = ak.macro_china_industrial_production()
            if df.empty:
                logger.warning("No industrial production data available")
                return {'industrial_production_yoy': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No industrial production data available for specified date range")
                return {'industrial_production_yoy': None}
            
            # Create time series with date index
            production_series = pd.Series(df_filtered['同比增长'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched industrial production growth rate time series")
            return {'industrial_production_yoy': production_series}
        except Exception as e:
            logger.error("Error fetching industrial production growth: %s", str(e))
            return {'industrial_production_yoy': None}

    def get_retail_sales(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's retail sales growth rate for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing retail sales growth rate time series
        """
        try:
            # Get retail sales data
            df = ak.macro_china_retail_sales()
            if df.empty:
                logger.warning("No retail sales data available")
                return {'retail_sales_yoy': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No retail sales data available for specified date range")
                return {'retail_sales_yoy': None}
            
            # Create time series with date index
            sales_series = pd.Series(df_filtered['同比增长'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched retail sales growth rate time series")
            return {'retail_sales_yoy': sales_series}
        except Exception as e:
            logger.error("Error fetching retail sales growth: %s", str(e))
            return {'retail_sales_yoy': None}

    def get_fixed_asset_investment(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's fixed asset investment growth rate for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing fixed asset investment growth rate time series
        """
        try:
            # Get fixed asset investment data
            df = ak.macro_china_fixed_asset_investment()
            if df.empty:
                logger.warning("No fixed asset investment data available")
                return {'fixed_asset_investment_yoy': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No fixed asset investment data available for specified date range")
                return {'fixed_asset_investment_yoy': None}
            
            # Create time series with date index
            investment_series = pd.Series(df_filtered['同比增长'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched fixed asset investment growth rate time series")
            return {'fixed_asset_investment_yoy': investment_series}
        except Exception as e:
            logger.error("Error fetching fixed asset investment growth: %s", str(e))
            return {'fixed_asset_investment_yoy': None}

    def get_trade_balance(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's trade data (exports and imports) for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing export and import growth rate time series
        """
        try:
            # Get trade data
            df = ak.macro_china_trade_balance()
            if df.empty:
                logger.warning("No trade data available")
                return {'exports_yoy': None, 'imports_yoy': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No trade data available for specified date range")
                return {'exports_yoy': None, 'imports_yoy': None}
            
            # Create time series with date index
            exports_series = pd.Series(df_filtered['出口同比增长'].astype(float).values, index=df_filtered['月份'])
            imports_series = pd.Series(df_filtered['进口同比增长'].astype(float).values, index=df_filtered['月份'])
            
            result = {
                'exports_yoy': exports_series,
                'imports_yoy': imports_series
            }
            logger.info("Successfully fetched trade data time series")
            return result
        except Exception as e:
            logger.error("Error fetching trade data: %s", str(e))
            return {'exports_yoy': None, 'imports_yoy': None}

    def get_cpi(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's Consumer Price Index (CPI) for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing CPI time series
        """
        try:
            # Get CPI data
            df = ak.macro_china_cpi()
            if df.empty:
                logger.warning("No CPI data available")
                return {'cpi_yoy': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No CPI data available for specified date range")
                return {'cpi_yoy': None}
            
            # Create time series with date index
            cpi_series = pd.Series(df_filtered['当月同比'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched CPI time series")
            return {'cpi_yoy': cpi_series}
        except Exception as e:
            logger.error("Error fetching CPI: %s", str(e))
            return {'cpi_yoy': None}

    def get_ppi(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's Producer Price Index (PPI) for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing PPI time series
        """
        try:
            # Get PPI data
            df = ak.macro_china_ppi()
            if df.empty:
                logger.warning("No PPI data available")
                return {'ppi_yoy': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No PPI data available for specified date range")
                return {'ppi_yoy': None}
            
            # Create time series with date index
            ppi_series = pd.Series(df_filtered['当月同比'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched PPI time series")
            return {'ppi_yoy': ppi_series}
        except Exception as e:
            logger.error("Error fetching PPI: %s", str(e))
            return {'ppi_yoy': None}

    def get_unemployment_rate(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's surveyed urban unemployment rate for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing unemployment rate time series
        """
        try:
            # Get unemployment rate data
            df = ak.macro_china_unemployment_rate()
            if df.empty:
                logger.warning("No unemployment rate data available")
                return {'unemployment_rate': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No unemployment rate data available for specified date range")
                return {'unemployment_rate': None}
            
            # Create time series with date index
            unemployment_series = pd.Series(df_filtered['城镇调查失业率'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched unemployment rate time series")
            return {'unemployment_rate': unemployment_series}
        except Exception as e:
            logger.error("Error fetching unemployment rate: %s", str(e))
            return {'unemployment_rate': None}

    def get_pmi(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's PMI indicators (Manufacturing and Non-Manufacturing) for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing manufacturing and non-manufacturing PMI time series
        """
        try:
            # Get PMI data
            df = ak.macro_china_pmi()
            if df.empty:
                logger.warning("No PMI data available")
                return {'manufacturing_pmi': None, 'non_manufacturing_pmi': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No PMI data available for specified date range")
                return {'manufacturing_pmi': None, 'non_manufacturing_pmi': None}
            
            # Create time series with date index
            manufacturing_series = pd.Series(df_filtered['制造业PMI'].astype(float).values, index=df_filtered['月份'])
            non_manufacturing_series = pd.Series(df_filtered['非制造业PMI'].astype(float).values, index=df_filtered['月份'])
            
            result = {
                'manufacturing_pmi': manufacturing_series,
                'non_manufacturing_pmi': non_manufacturing_series
            }
            logger.info("Successfully fetched PMI data time series")
            return result
        except Exception as e:
            logger.error("Error fetching PMI data: %s", str(e))
            return {'manufacturing_pmi': None, 'non_manufacturing_pmi': None}

    def get_social_financing(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's Total Social Financing (TSF) growth rate for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing TSF growth rate time series
        """
        try:
            # Get social financing data
            df = ak.macro_china_social_financing()
            if df.empty:
                logger.warning("No social financing data available")
                return {'social_financing_yoy': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No social financing data available for specified date range")
                return {'social_financing_yoy': None}
            
            # Create time series with date index
            financing_series = pd.Series(df_filtered['同比增长'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched social financing growth rate time series")
            return {'social_financing_yoy': financing_series}
        except Exception as e:
            logger.error("Error fetching social financing growth: %s", str(e))
            return {'social_financing_yoy': None}

    def get_m2_growth(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's M2 money supply growth rate for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing M2 growth rate time series
        """
        try:
            # Get M2 data
            df = ak.macro_china_m2()
            if df.empty:
                logger.warning("No M2 data available")
                return {'m2_yoy': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No M2 data available for specified date range")
                return {'m2_yoy': None}
            
            # Create time series with date index
            m2_series = pd.Series(df_filtered['同比增长'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched M2 growth rate time series")
            return {'m2_yoy': m2_series}
        except Exception as e:
            logger.error("Error fetching M2 growth: %s", str(e))
            return {'m2_yoy': None}

    def get_lpr(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's Loan Prime Rate (LPR) for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing 1-year and 5-year LPR time series
        """
        try:
            # Get LPR data
            df = ak.macro_china_lpr()
            if df.empty:
                logger.warning("No LPR data available")
                return {'lpr_1y': None, 'lpr_5y': None}
            
            # Convert date column to datetime
            df['日期'] = pd.to_datetime(df['日期'])
            
            # Filter by date range
            mask = (df['日期'] >= start_date) & (df['日期'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No LPR data available for specified date range")
                return {'lpr_1y': None, 'lpr_5y': None}
            
            # Create time series with date index
            lpr_1y_series = pd.Series(df_filtered['1年期LPR'].astype(float).values, index=df_filtered['日期'])
            lpr_5y_series = pd.Series(df_filtered['5年期LPR'].astype(float).values, index=df_filtered['日期'])
            
            result = {
                'lpr_1y': lpr_1y_series,
                'lpr_5y': lpr_5y_series
            }
            logger.info("Successfully fetched LPR time series")
            return result
        except Exception as e:
            logger.error("Error fetching LPR data: %s", str(e))
            return {'lpr_1y': None, 'lpr_5y': None}

    def get_housing_prices(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's new home prices growth rate for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing housing price growth rate time series
        """
        try:
            # Get housing price data
            df = ak.macro_china_housing_price()
            if df.empty:
                logger.warning("No housing price data available")
                return {'housing_price_yoy': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No housing price data available for specified date range")
                return {'housing_price_yoy': None}
            
            # Create time series with date index
            housing_series = pd.Series(df_filtered['同比增长'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched housing price growth rate time series")
            return {'housing_price_yoy': housing_series}
        except Exception as e:
            logger.error("Error fetching housing price growth: %s", str(e))
            return {'housing_price_yoy': None}

    def get_foreign_reserves(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch China's foreign exchange reserves for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing foreign reserves time series
        """
        try:
            # Get foreign reserves data
            df = ak.macro_china_foreign_reserves()
            if df.empty:
                logger.warning("No foreign reserves data available")
                return {'foreign_reserves': None}
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No foreign reserves data available for specified date range")
                return {'foreign_reserves': None}
            
            # Create time series with date index
            reserves_series = pd.Series(df_filtered['外汇储备'].astype(float).values, index=df_filtered['月份'])
            
            logger.info("Successfully fetched foreign reserves time series")
            return {'foreign_reserves': reserves_series}
        except Exception as e:
            logger.error("Error fetching foreign reserves: %s", str(e))
            return {'foreign_reserves': None}

    def get_us_pmi(self, start_date: datetime, end_date: datetime) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch US PMI indicators (Manufacturing, Services, and Composite) for the specified date range.
        
        Args:
            start_date (datetime): The start date for the observation period
            end_date (datetime): The end date for the observation period
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing manufacturing, services, and composite PMI time series
        """
        try:
            # Get US PMI data
            df = ak.macro_us_pmi()
            if df.empty:
                logger.warning("No US PMI data available")
                return {
                    'us_manufacturing_pmi': None,
                    'us_services_pmi': None,
                    'us_composite_pmi': None
                }
            
            # Convert date column to datetime
            df['月份'] = pd.to_datetime(df['月份'])
            
            # Filter by date range
            mask = (df['月份'] >= start_date) & (df['月份'] <= end_date)
            df_filtered = df[mask]
            
            if df_filtered.empty:
                logger.warning("No US PMI data available for specified date range")
                return {
                    'us_manufacturing_pmi': None,
                    'us_services_pmi': None,
                    'us_composite_pmi': None
                }
            
            # Create time series with date index
            manufacturing_series = pd.Series(df_filtered['制造业PMI'].astype(float).values, index=df_filtered['月份'])
            services_series = pd.Series(df_filtered['服务业PMI'].astype(float).values, index=df_filtered['月份'])
            composite_series = pd.Series(df_filtered['综合PMI'].astype(float).values, index=df_filtered['月份'])
            
            result = {
                'us_manufacturing_pmi': manufacturing_series,
                'us_services_pmi': services_series,
                'us_composite_pmi': composite_series
            }
            logger.info("Successfully fetched US PMI time series")
            return result
        except Exception as e:
            logger.error("Error fetching US PMI data: %s", str(e))
            return {
                'us_manufacturing_pmi': None,
                'us_services_pmi': None,
                'us_composite_pmi': None
            }

    def get_all_indicators(self, start_date: datetime = None, end_date: datetime = None) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch all available China economic indicators for the specified date range.
        
        Args:
            start_date (datetime, optional): The start date for the observation period.
                                          Defaults to 1 year ago if not specified.
            end_date (datetime, optional): The end date for the observation period.
                                        Defaults to current date if not specified.
            
        Returns:
            Dict[str, Optional[pd.Series]]: Dictionary containing all available economic indicators as time series
        """
        try:
            # Set default date range if not provided
            if start_date is None:
                start_date = datetime.now() - timedelta(days=365)
            if end_date is None:
                end_date = datetime.now()
                
            indicators = {}
            
            # GDP Growth
            gdp_data = self.get_gdp_growth(start_date, end_date)
            indicators['gdp_yoy'] = gdp_data['gdp_yoy']
            
            # Industrial Production
            industrial_production_data = self.get_industrial_production(start_date, end_date)
            indicators['industrial_production_yoy'] = industrial_production_data['industrial_production_yoy']
            
            # Retail Sales
            retail_sales_data = self.get_retail_sales(start_date, end_date)
            indicators['retail_sales_yoy'] = retail_sales_data['retail_sales_yoy']
            
            # Fixed Asset Investment
            fixed_asset_investment_data = self.get_fixed_asset_investment(start_date, end_date)
            indicators['fixed_asset_investment_yoy'] = fixed_asset_investment_data['fixed_asset_investment_yoy']
            
            # Trade Balance
            trade_data = self.get_trade_balance(start_date, end_date)
            indicators['exports_yoy'] = trade_data['exports_yoy']
            indicators['imports_yoy'] = trade_data['imports_yoy']
            
            # CPI
            cpi_data = self.get_cpi(start_date, end_date)
            indicators['cpi_yoy'] = cpi_data['cpi_yoy']
            
            # PPI
            ppi_data = self.get_ppi(start_date, end_date)
            indicators['ppi_yoy'] = ppi_data['ppi_yoy']
            
            # Unemployment Rate
            unemployment_data = self.get_unemployment_rate(start_date, end_date)
            indicators['unemployment_rate'] = unemployment_data['unemployment_rate']
            
            # PMI
            pmi_data = self.get_pmi(start_date, end_date)
            indicators['manufacturing_pmi'] = pmi_data['manufacturing_pmi']
            indicators['non_manufacturing_pmi'] = pmi_data['non_manufacturing_pmi']
            
            # Social Financing
            social_financing_data = self.get_social_financing(start_date, end_date)
            indicators['social_financing_yoy'] = social_financing_data['social_financing_yoy']
            
            # M2 Growth
            m2_data = self.get_m2_growth(start_date, end_date)
            indicators['m2_yoy'] = m2_data['m2_yoy']
            
            # LPR
            lpr_data = self.get_lpr(start_date, end_date)
            indicators['lpr_1y'] = lpr_data['lpr_1y']
            indicators['lpr_5y'] = lpr_data['lpr_5y']
            
            # Housing Prices
            housing_prices_data = self.get_housing_prices(start_date, end_date)
            indicators['housing_price_yoy'] = housing_prices_data['housing_price_yoy']
            
            # Foreign Reserves
            foreign_reserves_data = self.get_foreign_reserves(start_date, end_date)
            indicators['foreign_reserves'] = foreign_reserves_data['foreign_reserves']
            
            # US PMI
            us_pmi_data = self.get_us_pmi(start_date, end_date)
            indicators['us_manufacturing_pmi'] = us_pmi_data['us_manufacturing_pmi']
            indicators['us_services_pmi'] = us_pmi_data['us_services_pmi']
            indicators['us_composite_pmi'] = us_pmi_data['us_composite_pmi']
            
            logger.info("Successfully fetched all available indicators time series")
            return indicators
            
        except Exception as e:
            logger.error("Error fetching all indicators: %s", str(e))
            return {} 
     