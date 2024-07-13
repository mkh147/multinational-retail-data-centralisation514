from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import  DataCleaning

db_connector = DatabaseConnector()
db_extractor = DataExtractor()
dataframe = db_extractor.read_rds_table(db_connector.list_db_tables()[2], db_connector.init_db_engine())

data_cleaning = DataCleaning(dataframe)
cleaned_dataframe = data_cleaning.clean_user_data()

dataframe_upload = db_connector.upload_to_db(cleaned_dataframe, 'dim_users')

