import pandas as pd
from database_utils import DatabaseConnector  # Import the DatabaseConnector class

""" 
 This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
 The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
 """

class DataExtractor:
 
    def __init__(self, database_connector):
        # Assume database_connector is an instance of DatabaseConnector
        self.engine = database_connector.engine

    def read_rds_table(self, table_name):
        """Extracts the database table to a pandas DataFrame."""
        # Use Pandas to read the table into a DataFrame
        df = pd.read_sql_table(table_name, self.engine)
        return df
    
