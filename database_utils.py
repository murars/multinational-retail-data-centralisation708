from sqlalchemy import create_engine, inspect
import yaml

class DatabaseConnector:
        
    def __init__(self):
        self.creds = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self, filepath='db_creds.yaml'):
        """Reads database credentials from a YAML file """
        with open(filepath, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def init_db_engine(self):
        """Initialize and return an SQLAlchemy database engine using credentials."""
        creds = self.creds
        # PostgreSQL credentials
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine
      
    def upload_to_db(self, df, table_name, target_db_creds_path='target_db_creds.yaml'):
        """Uploads a Pandas DataFrame to a specified table in the target database."""
        # Read credentials for the target database
        target_creds = self.read_db_creds(target_db_creds_path)
        
        # Initialize connection to the target database
        target_db_url = f"postgresql://{target_creds['RDS_USER']}:{target_creds['RDS_PASSWORD']}@{target_creds['RDS_HOST']}:{target_creds['RDS_PORT']}/{target_creds['RDS_DATABASE']}"
        target_engine = create_engine(target_db_url)
        
        # Upload the DataFrame into the target database
        df.to_sql(name=table_name, con=target_engine, if_exists='append', index=False)
