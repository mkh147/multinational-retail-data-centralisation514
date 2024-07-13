import pandas as pd
import tabula

class DataExtractor:
    def __init__(self) -> None:
        pass

    @staticmethod
    def read_rds_table(table_name, engine):
        return pd.read_sql_table(table_name, engine)  
    
    def retrieve_pdf_data(self, link):
        dataframe = tabula.read_pdf("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        return dataframe
    
    def list_number_of_stores():
        pass
    