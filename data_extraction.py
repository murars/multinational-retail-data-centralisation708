import pandas as pd
import numpy as np
import requests
from database_utils import DatabaseConnector  # Import the DatabaseConnector class
from tabula import read_pdf
import boto3
from io import StringIO

""" 
 This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
 The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
 """

class DataExtractor:
 
    def __init__(self, database_connector=None, api_key=None):
        
        # Assume database_connector is an instance of DatabaseConnector
        if database_connector:
            self.engine = database_connector.engine 
            
    def read_rds_table(self, table_name):
        """Extracts the database table to a pandas DataFrame."""
        # Use Pandas to read the table into a DataFrame
        # Use tabula.read_pdf() or any other relevant method to extract data from the PDF
        # For example, assuming read_pdf can directly read from the link:
        df = pd.read_sql_table(table_name, self.engine)
        return df
    
    def retrieve_pdf_data(self, pdf_link):
        # Use tabula to read PDF from the link
        dfs = read_pdf(pdf_link, pages="all", multiple_tables=True)
        # Combine all tables into one DataFrame
        df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        return df
    
    def list_number_of_stores(self):
        headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            raise Exception("Failed to retrieve the number of stores")
    
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
    
    def extract_from_s3(self, s3_uri):
        s3 = boto3.client('s3')
        
        # Dynamically parse the s3_uri to extract bucket name and object key
        uri_parts = s3_uri.replace("s3://", "").split("/", 1)
        bucket_name, object_key = uri_parts[0], uri_parts[1]
        
        # Now using the dynamically determined bucket_name and object_key
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Convert the CSV content to a pandas DataFrame
        df = pd.read_csv(StringIO(csv_content))
        
        return df