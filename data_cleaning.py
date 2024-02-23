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
        for c in ['card_number']:
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
        
    @staticmethod
    def convert_product_weights(df):
        def convert_to_kg(weight):
            # Ensure the weight is a string
            weight = str(weight)
            # Pattern to match the numeric part and the unit
            pattern = re.compile(r'(\d+\.?\d*)\s*(kg|g|ml)', re.IGNORECASE)

            # Search for patterns indicating weights in 'kg', 'g', or 'ml'
            match = pattern.search(weight)
            if match:
                numeric_value, unit = match.groups()
                numeric_value = float(numeric_value)

                # Convert grams or milliliters to kilograms
                if unit.lower() in ['g', 'ml']:
                    converted_weight = numeric_value / 1000  # Convert to kg
                    return f"{converted_weight}kg"

                # If the unit is already 'kg', no conversion is needed, just return it
                elif unit.lower() == 'kg':
                    return f"{numeric_value}kg"

            # If the weight string doesn't match expected patterns (e.g., missing unit), return None
            return None

        # Apply the conversion to each weight entry in the DataFrame
        df['weight'] = df['weight'].apply(convert_to_kg)
        return df
    
    @staticmethod
    def clean_products_data(df):
     
        df.replace(to_replace=["N/A", "null", "N/a", "n/A", "NULL", "Null"], value=np.nan, inplace=True)
        # Fill NaN values with "Default Value"
        df.fillna(value="Default Value", inplace=True)
              
        df['product_price'] = df['product_price'].str.replace(r'[^\d.]', '', regex=True)      
        # Handle non-numeric 'product_price': replace non-numeric with NaN
        df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce')
        
        # Ensure 'date_added' is in correct datetime format (errors convert to NaT)
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        
        # Replace known erroneous values in 'category' and 'removed' with NaN
        erroneous_values = ['S1YB74MLMJ', 'C3NCA2CL35', 'WVPMHZP59U', 'T3QRRH7SRP', 'BPSADIOQOK', 'H5N71TV8AY']
        df['category'] = df['category'].replace(erroneous_values, np.nan)
        df['removed'] = df['removed'].replace(erroneous_values, np.nan)
        
        # This removes leading/trailing spaces and any trailing non-numeric characters (like '.' in '77g .')
        df['weight'] = df['weight'].str.strip()  # Remove leading/trailing whitespace
        df['weight'] = df['weight'].str.replace(r'[^\dkgmlKGML]+$', '', regex=True)  # Remove trailing non-numeric/non-unit characters
        return df
      
    @staticmethod
    def clean_orders_data(df):
        df.drop(["first_name", "last_name", "1"], axis=1, inplace=True)
        # axis=1 parameter specifies that pandas should look for these labels in the columns (not rows)
        
        df.replace(to_replace=["N/A", "null", "N/a", "n/A", "NULL", "Null"], value=np.nan, inplace=True)
        # Fill NaN values with "Default Value"
        df.fillna(value="Default Value", inplace=True)  
        return df 
    
    @staticmethod
    def clean_date_events(df):
        df.replace('NULL', np.nan, inplace=True)
        df.fillna(value="Default Value", inplace=True)
        return df 