import yaml 
import sqlalchemy

class DatabaseConnector:
    def __init__(self) -> None:
        pass

    def read_db_creds(self):
        with open("db_creds.yaml") as credentials:
            try:
                return(yaml.safe_load(credentials))
            except yaml.YAMLError as exc:
                print(exc)
    
    def init_db_engine(self):
       creds = self.read_db_creds()
       return sqlalchemy.create_engine(
           url =  f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
       )

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = sqlalchemy.inspect(engine)
        return inspector.get_table_names()




if __name__ == "__main__":
    database_connector = DatabaseConnector()
    print(database_connector.read_db_creds())
    print(database_connector.list_db_tables())

    