from config.logging_config import setup_logging
from data_service.macro_data_service import MacroDataService

def main():
    try:
        # Initialize the service
        macro_service = MacroDataService()
        
        # Get leading index datas
        leading_index = macro_service.get_leading_index()
        print("\nLeading Index Data (first 5 rows):")
        print(leading_index.head())
        
        # Get BBK index data
        bbk_index = macro_service.get_bbk_index()
        print("\nBBK Index Data (first 5 rows):")
        print(bbk_index.head())

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
