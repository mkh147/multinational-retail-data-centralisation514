import yaml 

class DatabaseConnector:
    def __init__(self) -> None:
        pass

    def read_db_creds(self):
        with open("db_creds.yaml") as credentials:
            try:
                return(yaml.safe_load(credentials))
            except yaml.YAMLError as exc:
                print(exc)

database_connector = DatabaseConnector()
print(database_connector.read_db_creds())