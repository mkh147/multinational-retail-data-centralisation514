import yaml
import sqlalchemy
import psycopg2

class DatabaseConnector:
    def __init__(self) -> None:
        pass

    def read_db_creds(self):
        try: 
            with open("db_creds.yaml", 'r') as credentials:
                return(yaml.safe_load(credentials))
        except yaml.YAMLError as exc:
                print(exc)

    def read_local_creds(self):
        try: 
            with open("local_creds.yaml", 'r') as credentials:
                return(yaml.safe_load(credentials))
        except yaml.YAMLError as exc:
                print(exc)
    
    def init_db_engine(self):
       db_creds = self.read_db_creds()
       engine = sqlalchemy.create_engine(f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}",isolation_level="AUTOCOMMIT")
       return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = sqlalchemy.inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table_name):
        db_creds = self.read_local_creds()
        engine = sqlalchemy.create_engine(f"{db_creds['DATABASE_TYPE']}+{db_creds['DBAPI']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}",isolation_level="AUTOCOMMIT")
        df.to_sql(table_name, con = engine, if_exists = 'replace') # use 'replace' if want to overwrite the existing table, 'fail' if you want to throw an error when attempting to overwrite       

if __name__ == "__main__":
    database_connector = DatabaseConnector()
    print(database_connector.read_db_creds())
    print(database_connector.list_db_tables())

    