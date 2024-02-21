import boto3
import pandas as pd
from io import StringIO

class DataExtractor:
    def __init__(self):
        # Initialize any required attributes here (if necessary)
        pass

    def extract_from_s3(self, s3_uri):
        # Assuming boto3 is set up with default credentials if needed
        s3 = boto3.client('s3')
        
        # Parse the S3 URI to get the bucket name and object key
        bucket_name = 'data-handling-public'
        object_key = 'products.csv'
        
        # Get the object from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        
        # Read the CSV content from the response
        csv_content = response['Body'].read().decode('utf-8')
        
        # Convert the CSV content to a pandas DataFrame
        df = pd.read_csv(StringIO(csv_content))
        
        return df

# Example usage
extractor = DataExtractor()
df = extractor.extract_from_s3('s3://data-handling-public/products.csv')
print(df['product_price'].head(10))
# print(df.head(10))
# print(df.info())
# print(df.describe())
# print(df['weight'].unique())





   