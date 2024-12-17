import yaml
from sqlalchemy import create_engine, text
import pandas as pd

class DatabaseConnector:
    def __init__(self, db_creds='/Users/carlajcostan/Documents/AI Core/multinational-retail-data-centralisation728/db_creds.yml'):
        """ 
        Initialises the DatabaseConnector with the path to the DB credentials containing file.

        Parameters:
        db_cred(str): The path to the YML file containing the credentials.
                      Defaults to db_creds.yml. 
        """
        self.db_creds = db_creds
        self.engine = self.init_db_engine()

    def _read_db_creds(self) -> dict:
        """
        Read the database credentials from the YAML file and return them as a dictionary.

        Returns:
        dict: A dictionary containing the database credentials.
        """
        with open(self.db_creds, 'r') as file:
            creds = yaml.safe_load(file)
        return creds    
    
    def init_db_engine(self):
        """
        Initialize and return an SQLAlchemy engine using the database credentials.

        Returns:
        sqlalchemy.engine.Engine: An SQLAlchemy engine object.
        """
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        creds = self._read_db_creds()
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self):
        """
        List all tables in the database.

        Returns:
        tables: A list of table names in the database.
        """
        with self.engine.connect() as connection:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            result = connection.execute(text(query))
            tables = [row[0] for row in result]
        return tables
    
    def get_table_columns(self, table_name):
        """
        Get the column names for a specific table in the database.

        Parameters:
        table_name (str): The name of the table to get columns from.

        Returns:
        columns: A list of column names in the table.
        """
        with self.engine.connect() as connection:
            query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
            result = connection.execute(text(query))
            columns = [row[0] for row in result]
        return columns
    
    def upload_to_db(self, df, table_name, database_name='sales_data', username='postgres', password='230200', host='localhost', port='5432'):
        """
        Upload a Pandas DataFrame to the specified table in the specified database.

        Parameters:
        df (pandas.DataFrame): The DataFrame to upload.
        table_name (str): The name of the table to upload the DataFrame to.
        database_name (str): The name of the database to connect to. Defaults to 'sales_data'.
        username (str): The username to connect to the database. Defaults to 'your_username'.
        password (str): The password to connect to the database. Defaults to 'your_password'.
        host (str): The host address of the database. Defaults to 'localhost'.
        port (str): The port number of the database. Defaults to '5432'.
        """
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{username}:{password}@{host}:{port}/{database_name}")

        with engine.connect() as connection:
            df.to_sql(table_name, connection, if_exists='replace', index=False)

if __name__ == "__main__":
    DatabaseConnector()