from sqlalchemy import create_engine, inspect
import yaml

"""you will use to connect with and upload data to the database."""
class DatabaseConnector:
        
    def __init__(self):
        self.creds = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self, filepath='db_creds.yaml'):
        """Reads database credentials from a YAML file and returns them as a dictionary."""
        with open(filepath, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def init_db_engine(self):
        """Initialize and return an SQLAlchemy database engine using credentials."""
        creds = self.creds
        # Construct the database URL (assuming PostgreSQL)
        db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        # Create and return the SQLAlchemy engine
        engine = create_engine(db_url)
        return engine
    
    def list_db_tables(self):
        """List all tables in the database."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        """
        Uploads a Pandas DataFrame to a specified table in the database.

        Parameters:
        - df: Pandas DataFrame to upload.
        - table_name: Name of the table to upload the DataFrame to.
        """
        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)
        
if __name__ == "__main__":
    db_connector = DatabaseConnector()
print(db_connector.list_db_tables())