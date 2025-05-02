from fastapi import APIRouter, HTTPException
from typing import Dict, List, Optional
from datetime import datetime
from service.macro_data_service import MacroDataService

router = APIRouter(
    prefix="/api/indicators",
    tags=["indicators"]
)

@router.get("/us/latest")
async def get_latest_us_economic_indicators() -> Dict:
    """
    Get the latest US economic indicators
    Returns:
        Dict: A dictionary containing the latest US economic indicators
    """
    try:
        service = MacroDataService()
        latest_indicators = service.get_all_us_latest_indicators()
        
        # Transform the indicators into the expected response format
        indicators_dict = {}
        for indicator in latest_indicators:
            # Convert snake_case to more readable format
            name = indicator.name.lower()
            indicators_dict[name] = indicator.value

        return {
            "indicators": indicators_dict,
            "status": "success"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/china/latest")
async def get_latest_china_economic_indicators() -> Dict:
    """
    Get the latest China economic indicators
    Returns:
        Dict: A dictionary containing the latest China economic indicators
    """
    try:
        service = MacroDataService()
        latest_indicators = service.get_all_china_latest_indicators()
        
        # Transform the indicators into the expected response format
        indicators_dict = {}
        for indicator in latest_indicators:
            # Convert snake_case to more readable format
            name = indicator.name.lower()
            indicators_dict[name] = indicator.value

        return {
            "indicators": indicators_dict,
            "status": "success"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 