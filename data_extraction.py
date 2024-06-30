import pandas as pd

class DataExtractor:
    def __init__(self) -> None:
        pass

    @staticmethod
    def read_rds_table(table_name, engine):
        return pd.read_sql_table(table_name, engine)  
    
