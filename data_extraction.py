import pandas as pd
import numpy as np
import requests
from database_utils import DatabaseConnector  # Import the DatabaseConnector class
from tabula import read_pdf
import boto3
from io import StringIO, BytesIO

class DataExtractor:
 
    def __init__(self, database_connector=None, api_key=None):
        
        if database_connector:
            self.engine = database_connector.engine 
            
    # Reading a table from RDS
    def read_rds_table(self, table_name):
        df = pd.read_sql_table(table_name, self.engine)
        return df
    
    # Retrieving PDF data
    def retrieve_pdf_data(self, data_link):
        dfs = read_pdf(data_link, pages="all", multiple_tables=True)
        # concotanate all pages in pdf 
        df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        return df
    
    # Extracting store data via API
    def list_number_of_stores(self):
        headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            raise Exception("Failed to retrieve the number of stores")
    
    # Extracting store data via API
    def retrieve_stores_data(self):
        number_of_stores = self.list_number_of_stores()
        stores_data = []
        headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        base_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

        for store_number in range(1, number_of_stores + 1):
            response = requests.get(f"{base_url}{store_number}", headers=headers)
            if response.status_code == 200:
                store_details = response.json()
                stores_data.append(store_details)

        return pd.DataFrame(stores_data)
    
    # Extracting CSV data from S3
    def extract_from_s3(self, data_link):
        s3 = boto3.client('s3')
        
        # Do dynamically parse ( not hard-coded )the s3_uri to extract bucket name and object key. 
        uri_parts = data_link.replace("s3://", "").split("/", 1)
        bucket_name, object_key = uri_parts[0], uri_parts[1]
        
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Convert the CSV content to a pandas DataFrame
        df = pd.read_csv(StringIO(csv_content))
        
        return df
    
    # Extraction JSON data from S3
    def extract_json_from_s3(self,data_link):
        # Directly read JSON content into a pandas DataFrame
        df = pd.read_json(data_link)
        return df

        