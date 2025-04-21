from config.logging_config import setup_logging
import logging
from service.macro_data_service import MacroDataService

def main():
    try:
        # Initialize the service
        macro_service = MacroDataService()
        
        macro_service.fetch_and_store_indicators()
      

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    setup_logging(logging.DEBUG)
    main()
