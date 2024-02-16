import pandas as pd
import numpy as np

class DataCleaning:
   
    @staticmethod
    def clean_user_data(df):
        # Handle NULL values with a placeholder for demonstration, specific fields might need different handling
        # Replace 'N/A' and similar representations of missing data with NaN
        df.replace("N/A", np.nan, inplace=True)
        df.replace("null", np.nan, inplace=True)  # Case sensitive, adjust as needed

        df = df.fillna(value="Default Value")

        # Correct date errors for 'join_date' and 'date_of_birth'
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
       
        # Ensuring 'phone_number' is treated as string to preserve data integrity
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')  

        # Correct 'eeEurope' and 'eeAmerica' values
        df['continent'] = df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
    
        # Replace nonsensical strings with 'Unknown' or np.nan
        nonsensical_values = ['1WZB1TE1HL', 'QMAVR5H3LD', 'GFJQ2AAEQ8', 'LU3E036ZD9', 'SLQBD982C0', '5586JCLARW', 'XQ953VS0FG']  # Add all 7 variations here
        df['continent'] = df['continent'].replace(nonsensical_values, np.nan)  # Or "Unknown"
    
        # Optionally, handle Null values if not already done
        df['continent'] = df['continent'].fillna("Unknown")  # Or np.nan, if you prefer to keep as null

        # Email validation step could be added here if necessary

        # Additional data cleaning steps can be added here based on specific needs

        return df