import yaml
import sqlalchemy
import psycopg2

class DatabaseConnector:
    """
    A class used to manage connections to a database, including reading credentials, initializing a database engine,
    listing tables, and uploading data to the database.
    """

    def __init__(self) -> None:
        """
        Initializes the DatabaseConnector class.
        """
        pass

    def read_db_creds(self):
        """
        Reads database credentials from a YAML file named 'db_creds.yaml'.

        Returns:
        dict: A dictionary containing the database credentials.

        Raises:
        yaml.YAMLError: If there is an error reading the YAML file.
        """
        try: 
            with open("db_creds.yaml", 'r') as credentials:
                return yaml.safe_load(credentials)
        except yaml.YAMLError as exc:
            print(exc)

    def read_local_creds(self):
        """
        Reads local database credentials from a YAML file named 'local_creds.yaml'.

        Returns:
        dict: A dictionary containing the local database credentials.

        Raises:
        yaml.YAMLError: If there is an error reading the YAML file.
        """
        try: 
            with open("local_creds.yaml", 'r') as credentials:
                return yaml.safe_load(credentials)
        except yaml.YAMLError as exc:
            print(exc)
    
    def init_db_engine(self):
        """
        Initializes a SQLAlchemy engine using the database credentials read from 'db_creds.yaml'.

        Returns:
        sqlalchemy.engine.base.Engine: The initialized SQLAlchemy engine.
        """
        db_creds = self.read_db_creds()
        engine = sqlalchemy.create_engine(
            f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}",
            isolation_level="AUTOCOMMIT"
        )
        return engine

    def list_db_tables(self):
        """
        Lists all tables in the database connected via the initialized engine.

        Returns:
        list: A list of table names in the database.
        """
        engine = self.init_db_engine()
        inspector = sqlalchemy.inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table_name):
        """
        Uploads a DataFrame to the database, replacing the table if it already exists.

        Parameters:
        df (pd.DataFrame): The DataFrame to upload.
        table_name (str): The name of the table in the database.

        Returns:
        None
        """
        db_creds = self.read_local_creds()
        engine = sqlalchemy.create_engine(
            f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}",
            isolation_level="AUTOCOMMIT"
        )
        df.to_sql(table_name, con=engine, if_exists='replace') # use 'replace' if you want to overwrite the existing table, 'fail' if you want to throw an error when attempting to overwrite       

if __name__ == "__main__":
    database_connector = DatabaseConnector()
    print(database_connector.read_db_creds())
    print(database_connector.list_db_tables())
