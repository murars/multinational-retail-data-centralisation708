import sys
import numpy as np
import pandas as pd

# Import your modules
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


def process_database_data():
    # Database connection
    db_connector = DatabaseConnector()
     
    # Extract data
    data_extractor = DataExtractor(db_connector)
    extracted_data = data_extractor.read_rds_table('legacy_users')
    
    # Clean data
    data_cleaning = DataCleaning()
    cleaned_data = data_cleaning.clean_user_data(extracted_data)
    
    # Upload data
    db_connector.upload_to_db(cleaned_data, 'dim_users')

def process_pdf_data(data_link):
    # retrieve pdf data from the link
    data_extractor = DataExtractor()
    df = data_extractor.retrieve_pdf_data(data_link)
    
    # cleaning
    data_cleaning = DataCleaning()
    cleaned_data = data_cleaning.clean_card_data(df)
    
    # uploading  
    db_connector = DatabaseConnector()
    db_connector.upload_to_db(cleaned_data, 'dim_card_details')

def process_store_data():
    # Extract store data
    data_extractor = DataExtractor()
    store_data = data_extractor.retrieve_stores_data()

    # Clean store data
    data_cleaning = DataCleaning()
    cleaned_store_data = data_cleaning.clean_store_data(store_data)

    # Upload the into the database
    db_connector = DatabaseConnector()
    db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')   
    
def process_product_data(data_link): 
    data_extractor = DataExtractor()
    # extraction
    df = data_extractor.extract_from_s3(data_link)
    
    # cleaning
    data_cleaner = DataCleaning()
    df = data_cleaner.clean_products_data(df)
    
    # converting weights to 'kg'
    df = data_cleaner.convert_product_weights(df)
    
    # upload the data to the database
    db_connector = DatabaseConnector()
    db_connector.upload_to_db(df, 'dim_products')
    
def process_orders_data():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor(db_connector)
    extracted_data = data_extractor.read_rds_table('orders_table')
    data_cleaning = DataCleaning()
    cleaned_data = data_cleaning.clean_orders_data(extracted_data)
    db_connector.upload_to_db(cleaned_data, 'orders_table')
    
def process_date_events_data(data_link):
    # etraction
    data_extractor = DataExtractor()
    df = data_extractor.extract_json_from_s3(data_link) 
    
    # celeaning
    data_cleaner = DataCleaning()
    df = data_cleaner.clean_date_events(df)
    # upload the data to the 
    db_connector = DatabaseConnector()
    db_connector.upload_to_db(df, 'dim_date_times')
   
def main():
    pdf_link_default = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"  # Modular PDF link definition
    csv_link_default = "s3://data-handling-public/products.csv"
    json_link_default = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    
    # Determine which task to run and check if a link given by user.
    if len(sys.argv) > 1:
        task = sys.argv[1]

        # which data source link to use based on the task
        if task in ["pdf", "csv", "json"]:
            data_link = sys.argv[2] if len(sys.argv) > 2 else locals()[f"{task}_link_default"]

        # process tasks
        if task == "process_user_data":
            process_database_data()
        elif task == "pdf":
            process_pdf_data(data_link)
        elif task == "api":
            process_store_data()
        elif task == "csv":
            process_product_data(data_link)
        elif task == "process_orders_data":
            process_orders_data()
        elif task == "json":
            process_date_events_data(data_link) 
        else:
            print(f"Unrecognized task '{task}'.")
            sys.exit(1)

    else:
        print("No task, please specify a task.")
        sys.exit(1)

if __name__ == "__main__":
    main()
    