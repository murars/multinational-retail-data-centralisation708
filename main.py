from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Step 1: Connect to the database
    db_connector = DatabaseConnector()
    
    # # Optional: List tables (for verification or informational purposes)
    # print("Available tables:", db_connector.list_db_tables())
    
    # Step 2: Extract data
    data_extractor = DataExtractor(db_connector)
    extracted_data = data_extractor.read_rds_table('legacy_store_details')
    
    # Step 3: Clean data
    data_cleaning = DataCleaning()
    cleaned_data = data_cleaning.clean_user_data(extracted_data)
    
    # Step 4: Upload cleaned data
    db_connector.upload_to_db(cleaned_data, 'dim_store_details')

if __name__ == "__main__":
    main()