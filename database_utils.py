import yaml
import sqlalchemy

class DatabaseConnector:
    def __init__(self) -> None:
        pass

    def read_db_creds(self):
        with open("db_creds.yaml", 'r') as credentials:
            try:
                return(yaml.safe_load(credentials))
            except yaml.YAMLError as exc:
                print(exc)
    
    def init_db_engine(self):
       creds = self.read_db_creds()
       engine = sqlalchemy.create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
       return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = sqlalchemy.inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        df.to_sql(table_name, engine,if_exists='replace')        

if __name__ == "__main__":
    database_connector = DatabaseConnector()
    print(database_connector.read_db_creds())
    print(database_connector.list_db_tables())

    