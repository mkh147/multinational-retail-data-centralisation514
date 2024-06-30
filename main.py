from database_utils import DatabaseConnector
from data_extraction import DataExtractor

connection = DatabaseConnector()


extractor = DataExtractor()

df = extractor.read_rds_table(connection.list_db_tables()[2], connection.init_db_engine())
print(df)



