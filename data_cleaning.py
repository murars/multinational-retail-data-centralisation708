import pandas as pd
import numpy as np
import re
class DataCleaning:
   
    @staticmethod
    def clean_user_data(df):
        # Handle NULL values with a placeholder for demonstration, specific fields might need different handling
        # Replace 'N/A' and similar representations of missing data with NaN
        df.replace(["N/A", "null", "Null", "NULL"], np.nan, inplace=True)
        df = df.fillna(value="Default Value")
        # Correct date errors for 'join_date' and 'date_of_birth'
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        # Standardize and trim company names
        df['company'] = df['company'].str.strip().str.title() 
        # Validate email
        df['email_address'] = df['email_address'].apply(lambda x: x if re.match(r"[^@]+@[^@]+\.[^@]+", x) else np.nan)  
        df['address'] = df['address'].str.strip()  # Trim addresses
        # Standardize phone numbers to digits only
        df['phone_number'] = df['phone_number'].str.replace(r'\D', '', regex=True)  
        # Parse and standardize join dates
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')       
                
    @staticmethod
    def clean_card_data(df):
        # Implement cleaning logic here
        df.replace(["NULL", "Null", "null"], np.nan, inplace=True)
        df.fillna('DefaultValue', inplace=True)
        
        # Example: Replace '?' in card_number with 'Unknown'
        df['card_number'] = df['card_number'].astype(str)
        df['card_number'] = df['card_number'].apply(lambda x: 'Unknown' if '?' in x else x)
        # Safe conversion to numeric, with error handling - python urged us like below;
        #FutureWarning: errors='ignore' is deprecated and will raise in a future version. 
        for c in ['card_number', 'another_column']:
            try:
                df[c] = pd.to_numeric(df[c])
            except ValueError:
                # Handle the exception, for example, by logging or using a default value
                # This could also be a place to mark these rows for review or removal
                print(f"Non-numeric values found in {c}, replacing with NaN")
                df[c] = pd.to_numeric(df[c], errors='coerce')  # Convert problematic values to NaN
                 
        # Convert expiry_date to datetime, assuming format is MM/YY
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        
        # Convert date_payment_confirmed to datetime
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        
    @staticmethod
    def clean_store_data(df):
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
    
        return df
        
        
        