from datetime import datetime, timedelta
from client.akshare_finance_client import AkshareFinanceClient

# write main function to test the get_gdp_growth function
if __name__ == "__main__":
    client = AkshareFinanceClient()
    indicators = client.get_gdp_growth('2020-01-01', '2025-01-01')
    print(indicators)