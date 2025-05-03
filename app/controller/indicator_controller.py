from fastapi import APIRouter, HTTPException, Query, Body
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from service.macro_data_service import MacroDataService
from config.logging_config import get_logger
from pydantic import BaseModel

logger = get_logger(__name__)

router = APIRouter(
    prefix="/api/indicators",
    tags=["indicators"]
)

class DateRangeRequest(BaseModel):
    start_date: datetime
    end_date: datetime

@router.get("/us")
async def get_us_economic_indicators(
    start_date: datetime = Query(default_factory=lambda: datetime.now() - timedelta(days=30)),
    end_date: datetime = Query(default_factory=lambda: datetime.now())
) -> Dict:
    """
    Get US economic indicators within a date range
    
    Args:
        start_date: Start date for the indicators (defaults to 30 days ago)
        end_date: End date for the indicators (defaults to current date)
    
    Returns:
        Dict: A dictionary containing the US economic indicators within the specified date range
    """
    try:
        logger.info("Fetching US economic indicators from %s to %s", 
                   start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        service = MacroDataService()
        indicators = service.get_all_us_indicators(start_date, end_date)
        
        response = {
            "indicators": indicators,
            "status": "success"
        }
        logger.info("Successfully retrieved %d US economic indicators", len(indicators))
        return response
    except Exception as e:
        logger.error("Error fetching US economic indicators: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/china")
async def get_china_economic_indicators(
    start_date: datetime = Query(default_factory=lambda: datetime.now() - timedelta(days=30)),
    end_date: datetime = Query(default_factory=lambda: datetime.now())
) -> Dict:
    """
    Get China economic indicators within a date range
    
    Args:
        start_date: Start date for the indicators (defaults to 30 days ago)
        end_date: End date for the indicators (defaults to current date)
    
    Returns:
        Dict: A dictionary containing the China economic indicators within the specified date range
    """
    try:
        logger.info("Fetching China economic indicators from %s to %s", 
                   start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        service = MacroDataService()
        indicators = service.get_all_china_indicators(start_date, end_date)
        
        response = {
            "indicators": indicators,
            "status": "success",
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        }
        logger.info("Successfully retrieved %d China economic indicators", len(indicators))
        return response
    except Exception as e:
        logger.error("Error fetching China economic indicators: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/fetch-store-all-macro-indices")
async def fetch_and_store_all_macro_indices(
    date_range: DateRangeRequest = Body(...)
) -> Dict:
    """
    Fetch and store all indices within a date range
    
    Args:
        date_range: Object containing start_date and end_date for fetching indices
    
    Returns:
        Dict: A dictionary containing the status of each index fetch operation
    """
    try:
        logger.info("Fetching all indices from %s to %s", 
                   date_range.start_date.strftime('%Y-%m-%d'), 
                   date_range.end_date.strftime('%Y-%m-%d'))
        service = MacroDataService()
        
        # Fetch and store all different types of indices
        results = {
            "leading_indicators": service.fetch_and_store_leading_indicators_by_date_range(date_range.start_date, date_range.end_date),
            "consumer_indices": service.fetch_and_store_consumer_indices_by_date_range(date_range.start_date, date_range.end_date),
            "financial_condition_indices": service.fetch_and_store_financial_condition_indices_by_date_range(date_range.start_date, date_range.end_date),
            "treasury_data": service.fetch_and_store_treasury_data_by_date_range(date_range.start_date, date_range.end_date),
            "pmi_indicators": service.fetch_and_store_pmi_indicators_by_date_range(date_range.start_date, date_range.end_date),
            "commodity_prices": service.fetch_and_store_commodity_prices_by_date_range(date_range.start_date, date_range.end_date)
        }
        
        response = {
            "status": "success",
            "results": results
        }
        logger.info("Successfully completed fetching and storing all indices")
        return response
    except Exception as e:
        logger.error("Error in fetch_and_store_all_indices: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))
