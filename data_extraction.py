import pandas as pd
import numpy as np
from database_utils import DatabaseConnector  # Import the DatabaseConnector class
from tabula import read_pdf


""" 
 This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
 The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
 """

class DataExtractor:
 
    def __init__(self, database_connector=None):
        # Assume database_connector is an instance of DatabaseConnector
        self.engine = database_connector.engine if database_connector else None

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