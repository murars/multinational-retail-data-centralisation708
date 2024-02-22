import sys
import numpy as np
import pandas as pd

# Import your modules
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


# Define functions for each task
def process_database_data():
    # Database processing logic here
     # Step 1: Connect to the database
    db_connector = DatabaseConnector()
    
    # # Optional: List tables (for verification or informational purposes)
    # print("Available tables:", db_connector.list_db_tables())
    
    # Step 2: Extract data
    data_extractor = DataExtractor(db_connector)
    extracted_data = data_extractor.read_rds_table('legacy_users')
    
    # Step 3: Clean data
    data_cleaning = DataCleaning()
    cleaned_data = data_cleaning.clean_user_data(extracted_data)
    
    # Step 4: Upload cleaned data
    db_connector.upload_to_db(cleaned_data, 'dim_users')

def process_pdf_data(pdf_link):
    # Initialize DataExtractor with the link to the PDF
    data_extractor = DataExtractor()
    df = data_extractor.retrieve_pdf_data(pdf_link)
    
    # PDF processing logic here
    # Clean data
    data_cleaning = DataCleaning()
    cleaned_data = data_cleaning.clean_card_data(df)
    
    # Upload cleaned data
    db_connector = DatabaseConnector()
    db_connector.upload_to_db(cleaned_data, 'dim_card_details')

def process_store_data():
    # Step 1: Extract store data
    data_extractor = DataExtractor()
    store_data = data_extractor.retrieve_stores_data()

    # Step 2: Clean store data
    data_cleaning = DataCleaning()
    cleaned_store_data = data_cleaning.clean_store_data(store_data)

    # Step 3: Upload cleaned data to the database
    db_connector = DatabaseConnector()
    db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')   
    
def process_product_data(s3_uri): 
    data_extractor = DataExtractor()
    # Pass the s3_uri to the method; no need to pass df
    df = data_extractor.extract_from_s3(s3_uri)
    
    # Instantiate the DataCleaning class
    data_cleaner = DataCleaning()
    
    # First, clean the products data in general
    df = data_cleaner.clean_products_data(df)
    
    # Then, convert product weights to a consistent unit
    df = data_cleaner.convert_product_weights(df)
    
    # Finally, upload the cleaned and transformed data to your database
    db_connector = DatabaseConnector()
    db_connector.upload_to_db(df, 'dim_products')
    
def process_orders_data():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    extracted_data = data_extractor.read_rds_table('orders_table')
    data_cleaning = DataCleaning()
    cleaned_data = data_cleaning.clean_orders_data(extracted_data)
    db_connector.upload_to_db(cleaned_data, 'orders_table')
    
def process_date_evets_data():
    json_s3_uri = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    data_extractor = DataExtractor()
    df = data_extractor.extract_json_from_s3(json_s3_uri)  # Extract JSON data into a DataFrame
    # Instantiate the DataCleaning class
    data_cleaner = DataCleaning()
    # First, clean the products data in general
    df = data_cleaner.clean_date_events(df)
    # Finally, upload the cleaned and transformed data to your database
    db_connector = DatabaseConnector()
    db_connector.upload_to_db(df, 'dim_date_times')
   
def main():
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"  # Modular PDF link definition
    s3_uri = "s3://data-handling-public/products.csv"
    # Determine which task to run based on command-line arguments
    if len(sys.argv) > 1:
        task = sys.argv[1]
        if task == "process_user_data":
            process_database_data()
        elif task == "pdf" : 
            # Use the provided PDF link if specified, else use the default
            link = sys.argv[2] if len(sys.argv) > 2 else pdf_link
            process_pdf_data(link)
        elif task == "api":
            process_store_data()
        elif task == "csv":
            process_product_data(s3_uri)
        elif task == "process_orders_data":
            process_orders_data()
        elif task == "process_date_evets_data":
            process_date_evets_data()
        else:
            print(f"Unrecognized task '{task}'.")  # Updated to handle unrecognized tasks
    else:
        print("Available tasks: database, pdf, api, csv")
        sys.exit(1)  # Terminate the program with an error status
if __name__ == "__main__":
    main()
    
 