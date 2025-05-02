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

    def get_gdp_growth(self) -> Optional[float]:
        """
        Fetch China's quarterly GDP growth rate.
        
        Returns:
            Optional[float]: The latest GDP growth rate, or None if unavailable
        """
        try:
            # Get GDP data
            df = ak.macro_china_gdp()
            if df.empty:
                logger.warning("No GDP data available")
                return None
            
            # Get the latest GDP growth rate
            latest_growth = df.iloc[-1]['GDP同比增长']
            logger.info("Successfully fetched GDP growth rate: %s", latest_growth)
            return float(latest_growth)
        except Exception as e:
            logger.error("Error fetching GDP growth rate: %s", str(e))
            return None

    def get_industrial_production(self) -> Optional[float]:
        """
        Fetch China's industrial production growth rate.
        
        Returns:
            Optional[float]: The latest industrial production growth rate, or None if unavailable
        """
        try:
            # Get industrial production data
            df = ak.macro_china_industrial_production()
            if df.empty:
                logger.warning("No industrial production data available")
                return None
            
            # Get the latest growth rate
            latest_growth = df.iloc[-1]['同比增长']
            logger.info("Successfully fetched industrial production growth: %s", latest_growth)
            return float(latest_growth)
        except Exception as e:
            logger.error("Error fetching industrial production growth: %s", str(e))
            return None

    def get_retail_sales(self) -> Optional[float]:
        """
        Fetch China's retail sales growth rate.
        
        Returns:
            Optional[float]: The latest retail sales growth rate, or None if unavailable
        """
        try:
            # Get retail sales data
            df = ak.macro_china_retail_sales()
            if df.empty:
                logger.warning("No retail sales data available")
                return None
            
            # Get the latest growth rate
            latest_growth = df.iloc[-1]['同比增长']
            logger.info("Successfully fetched retail sales growth: %s", latest_growth)
            return float(latest_growth)
        except Exception as e:
            logger.error("Error fetching retail sales growth: %s", str(e))
            return None

    def get_fixed_asset_investment(self) -> Optional[float]:
        """
        Fetch China's fixed asset investment growth rate.
        
        Returns:
            Optional[float]: The latest fixed asset investment growth rate, or None if unavailable
        """
        try:
            # Get fixed asset investment data
            df = ak.macro_china_fixed_asset_investment()
            if df.empty:
                logger.warning("No fixed asset investment data available")
                return None
            
            # Get the latest growth rate
            latest_growth = df.iloc[-1]['同比增长']
            logger.info("Successfully fetched fixed asset investment growth: %s", latest_growth)
            return float(latest_growth)
        except Exception as e:
            logger.error("Error fetching fixed asset investment growth: %s", str(e))
            return None

    def get_trade_balance(self) -> Dict[str, Optional[float]]:
        """
        Fetch China's trade data (exports and imports).
        
        Returns:
            Dict[str, Optional[float]]: Dictionary containing export and import growth rates
        """
        try:
            # Get trade data
            df = ak.macro_china_trade_balance()
            if df.empty:
                logger.warning("No trade data available")
                return {'exports': None, 'imports': None}
            
            # Get the latest export and import growth rates
            latest_exports = df.iloc[-1]['出口同比增长']
            latest_imports = df.iloc[-1]['进口同比增长']
            
            result = {
                'exports': float(latest_exports),
                'imports': float(latest_imports)
            }
            logger.info("Successfully fetched trade data: %s", result)
            return result
        except Exception as e:
            logger.error("Error fetching trade data: %s", str(e))
            return {'exports': None, 'imports': None}

    def get_cpi(self) -> Optional[float]:
        """
        Fetch China's Consumer Price Index (CPI).
        
        Returns:
            Optional[float]: The latest CPI value, or None if unavailable
        """
        try:
            # Get CPI data
            df = ak.macro_china_cpi()
            if df.empty:
                logger.warning("No CPI data available")
                return None
            
            # Get the latest CPI value
            latest_cpi = df.iloc[-1]['当月同比']
            logger.info("Successfully fetched CPI: %s", latest_cpi)
            return float(latest_cpi)
        except Exception as e:
            logger.error("Error fetching CPI: %s", str(e))
            return None

    def get_ppi(self) -> Optional[float]:
        """
        Fetch China's Producer Price Index (PPI).
        
        Returns:
            Optional[float]: The latest PPI value, or None if unavailable
        """
        try:
            # Get PPI data
            df = ak.macro_china_ppi()
            if df.empty:
                logger.warning("No PPI data available")
                return None
            
            # Get the latest PPI value
            latest_ppi = df.iloc[-1]['当月同比']
            logger.info("Successfully fetched PPI: %s", latest_ppi)
            return float(latest_ppi)
        except Exception as e:
            logger.error("Error fetching PPI: %s", str(e))
            return None

    def get_unemployment_rate(self) -> Optional[float]:
        """
        Fetch China's surveyed urban unemployment rate.
        
        Returns:
            Optional[float]: The latest unemployment rate, or None if unavailable
        """
        try:
            # Get unemployment rate data
            df = ak.macro_china_unemployment_rate()
            if df.empty:
                logger.warning("No unemployment rate data available")
                return None
            
            # Get the latest unemployment rate
            latest_rate = df.iloc[-1]['城镇调查失业率']
            logger.info("Successfully fetched unemployment rate: %s", latest_rate)
            return float(latest_rate)
        except Exception as e:
            logger.error("Error fetching unemployment rate: %s", str(e))
            return None

    def get_pmi(self) -> Dict[str, Optional[float]]:
        """
        Fetch China's PMI indicators (Manufacturing and Non-Manufacturing).
        
        Returns:
            Dict[str, Optional[float]]: Dictionary containing manufacturing and non-manufacturing PMI
        """
        try:
            # Get PMI data
            df = ak.macro_china_pmi()
            if df.empty:
                logger.warning("No PMI data available")
                return {'manufacturing': None, 'non_manufacturing': None}
            
            # Get the latest PMI values
            latest_manufacturing = df.iloc[-1]['制造业PMI']
            latest_non_manufacturing = df.iloc[-1]['非制造业PMI']
            
            result = {
                'manufacturing': float(latest_manufacturing),
                'non_manufacturing': float(latest_non_manufacturing)
            }
            logger.info("Successfully fetched PMI data: %s", result)
            return result
        except Exception as e:
            logger.error("Error fetching PMI data: %s", str(e))
            return {'manufacturing': None, 'non_manufacturing': None}

    def get_social_financing(self) -> Optional[float]:
        """
        Fetch China's Total Social Financing (TSF) growth rate.
        
        Returns:
            Optional[float]: The latest TSF growth rate, or None if unavailable
        """
        try:
            # Get social financing data
            df = ak.macro_china_social_financing()
            if df.empty:
                logger.warning("No social financing data available")
                return None
            
            # Get the latest growth rate
            latest_growth = df.iloc[-1]['同比增长']
            logger.info("Successfully fetched social financing growth: %s", latest_growth)
            return float(latest_growth)
        except Exception as e:
            logger.error("Error fetching social financing growth: %s", str(e))
            return None

    def get_m2_growth(self) -> Optional[float]:
        """
        Fetch China's M2 money supply growth rate.
        
        Returns:
            Optional[float]: The latest M2 growth rate, or None if unavailable
        """
        try:
            # Get M2 data
            df = ak.macro_china_m2()
            if df.empty:
                logger.warning("No M2 data available")
                return None
            
            # Get the latest growth rate
            latest_growth = df.iloc[-1]['同比增长']
            logger.info("Successfully fetched M2 growth: %s", latest_growth)
            return float(latest_growth)
        except Exception as e:
            logger.error("Error fetching M2 growth: %s", str(e))
            return None

    def get_lpr(self) -> Dict[str, Optional[float]]:
        """
        Fetch China's Loan Prime Rate (LPR).
        
        Returns:
            Dict[str, Optional[float]]: Dictionary containing 1-year and 5-year LPR
        """
        try:
            # Get LPR data
            df = ak.macro_china_lpr()
            if df.empty:
                logger.warning("No LPR data available")
                return {'1y': None, '5y': None}
            
            # Get the latest LPR values
            latest_1y = df.iloc[-1]['1年期LPR']
            latest_5y = df.iloc[-1]['5年期LPR']
            
            result = {
                '1y': float(latest_1y),
                '5y': float(latest_5y)
            }
            logger.info("Successfully fetched LPR data: %s", result)
            return result
        except Exception as e:
            logger.error("Error fetching LPR data: %s", str(e))
            return {'1y': None, '5y': None}

    def get_housing_prices(self) -> Optional[float]:
        """
        Fetch China's new home prices growth rate.
        
        Returns:
            Optional[float]: The latest housing price growth rate, or None if unavailable
        """
        try:
            # Get housing price data
            df = ak.macro_china_housing_price()
            if df.empty:
                logger.warning("No housing price data available")
                return None
            
            # Get the latest growth rate
            latest_growth = df.iloc[-1]['同比增长']
            logger.info("Successfully fetched housing price growth: %s", latest_growth)
            return float(latest_growth)
        except Exception as e:
            logger.error("Error fetching housing price growth: %s", str(e))
            return None

    def get_foreign_reserves(self) -> Optional[float]:
        """
        Fetch China's foreign exchange reserves.
        
        Returns:
            Optional[float]: The latest foreign reserves value, or None if unavailable
        """
        try:
            # Get foreign reserves data
            df = ak.macro_china_foreign_reserves()
            if df.empty:
                logger.warning("No foreign reserves data available")
                return None
            
            # Get the latest value
            latest_value = df.iloc[-1]['外汇储备']
            logger.info("Successfully fetched foreign reserves: %s", latest_value)
            return float(latest_value)
        except Exception as e:
            logger.error("Error fetching foreign reserves: %s", str(e))
            return None

    def get_us_pmi(self) -> Dict[str, Optional[float]]:
        """
        Fetch US PMI indicators (Manufacturing, Services, and Composite).
        
        Returns:
            Dict[str, Optional[float]]: Dictionary containing manufacturing, services, and composite PMI
        """
        try:
            # Get US PMI data
            df = ak.macro_us_pmi()
            if df.empty:
                logger.warning("No US PMI data available")
                return {'manufacturing': None, 'services': None, 'composite': None}
            
            # Get the latest PMI values
            latest_manufacturing = df.iloc[-1]['制造业PMI']
            latest_services = df.iloc[-1]['服务业PMI']
            latest_composite = df.iloc[-1]['综合PMI']
            
            result = {
                'manufacturing': float(latest_manufacturing),
                'services': float(latest_services),
                'composite': float(latest_composite)
            }
            logger.info("Successfully fetched US PMI data: %s", result)
            return result
        except Exception as e:
            logger.error("Error fetching US PMI data: %s", str(e))
            return {'manufacturing': None, 'services': None, 'composite': None}

    def get_all_indicators(self) -> Dict[str, Optional[float]]:
        """
        Fetch all available China economic indicators.
        
        Returns:
            Dict[str, Optional[float]]: Dictionary containing all available economic indicators
        """
        try:
            indicators = {}
            
            # GDP Growth
            indicators['gdp_growth'] = self.get_gdp_growth()
            
            # Industrial Production
            indicators['industrial_production'] = self.get_industrial_production()
            
            # Retail Sales
            indicators['retail_sales'] = self.get_retail_sales()
            
            # Fixed Asset Investment
            indicators['fixed_asset_investment'] = self.get_fixed_asset_investment()
            
            # Trade Balance
            trade_data = self.get_trade_balance()
            indicators['exports'] = trade_data['exports']
            indicators['imports'] = trade_data['imports']
            
            # CPI
            indicators['cpi'] = self.get_cpi()
            
            # PPI
            indicators['ppi'] = self.get_ppi()
            
            # Unemployment Rate
            indicators['unemployment_rate'] = self.get_unemployment_rate()
            
            # PMI
            pmi_data = self.get_pmi()
            indicators['manufacturing_pmi'] = pmi_data['manufacturing']
            indicators['non_manufacturing_pmi'] = pmi_data['non_manufacturing']
            
            # Social Financing
            indicators['social_financing'] = self.get_social_financing()
            
            # M2 Growth
            indicators['m2_growth'] = self.get_m2_growth()
            
            # LPR
            lpr_data = self.get_lpr()
            indicators['lpr_1y'] = lpr_data['1y']
            indicators['lpr_5y'] = lpr_data['5y']
            
            # Housing Prices
            indicators['housing_prices'] = self.get_housing_prices()
            
            # Foreign Reserves
            indicators['foreign_reserves'] = self.get_foreign_reserves()
            
            # US PMI
            us_pmi_data = self.get_us_pmi()
            indicators['us_manufacturing_pmi'] = us_pmi_data['manufacturing']
            indicators['us_services_pmi'] = us_pmi_data['services']
            indicators['us_composite_pmi'] = us_pmi_data['composite']
            
            logger.info("Successfully fetched all available indicators")
            return indicators
            
        except Exception as e:
            logger.error("Error fetching all indicators: %s", str(e))
            return {} 