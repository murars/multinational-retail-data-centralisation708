from sqlalchemy import pandas as pd

class DataCleaning:
    """ 
     this script will contain a class DataCleaning with methods to clean data from each of the data sources.
    """
    class DataCleaning:
        @staticmethod
        def clean_user_data(df):
            # Handle NULL values
            # You might choose to fill them with a default value, or drop rows/columns
            df = df.fillna(value="Default Value")  # Example: Fill NA/NaN with a default value
            # df.dropna(inplace=True)  # Alternatively, drop rows with NaN values

            # Correct date errors
            # Assuming 'date_column' is the name of your date column
            df['date_column'] = pd.to_datetime(df['date_column'], errors='coerce')

            # Correct incorrectly typed values
            # For example, ensuring a column is treated as a specific type
            df['numeric_column'] = pd.to_numeric(df['numeric_column'], errors='coerce')

            # Identify and handle rows with wrong information
            # This step highly depends on your data and might require specific logic
            # Example: Removing rows where 'age' is unreasonably high
            df = df[df['age'] <= 100]  # Adjust the condition based on your data

            return df
        
    