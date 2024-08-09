from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import  DataCleaning
import pandas as pd
from pandasgui import show

db_connector = DatabaseConnector()
db_extractor = DataExtractor()
db_cleaning = DataCleaning()

dataframe = db_extractor.read_rds_table(db_connector.list_db_tables()[2], db_connector.init_db_engine())

cleaned_dataframe = data_cleaning.clean_user_data(dataframe)


db_connector.upload_to_db(cleaned_dataframe, 'dim_users')

card_data_link ="https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

card_data_df = db_extractor.retrieve_pdf_data(card_data_link)
card_data_df.to_csv('card_data.csv')

card_data_df = pd.read_csv('card_data.csv')
card_data_df_cleaned = db_cleaning.clean_card_data(card_data_df)
card_data_df_cleaned.to_csv('card_data_cleaned.csv')

db_connector.upload_to_db(card_data_df_cleaned, 'dim_card_details')

number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"

number_of_stores = db_extractor.list_number_of_stores(number_of_stores_endpoint, api_key)
print(number_of_stores)
stores_df = db_extractor.retrieve_stores_data(store_details_endpoint, api_key, number_of_stores)
stores_df.to_csv('stores_data.csv')

stores_df = pd.read_csv('stores_data.csv')
stores_df_cleaned = db_cleaning.clean_store_data(stores_df)
db_connector.upload_to_db(stores_df_cleaned, 'dim_store_details')
stores_df_cleaned.to_csv('stores_data_cleaned.csv')

s3_address = 's3://data-handling-public/products.csv'
products_df = db_extractor.extract_from_s3(s3_address)
products_df.to_csv('products_data.csv')

products_df = pd.read_csv('products_data.csv')
products_df_dropped = db_cleaning.clean_products_data(products_df)
products_df_cleaned = db_cleaning.convert_product_weights(products_df_dropped)
products_df_cleaned.to_csv('products_data_cleaned.csv')
db_connector.upload_to_db(products_df_cleaned, 'dim_products')

tables = db_connector.list_db_tables()
print(tables)
orders_data = db_extractor.read_rds_table(db_connector.list_db_tables()[3], db_connector.init_db_engine())
orders_data.to_csv('orders_data.csv')

orders_df = pd.read_csv('orders_data.csv')

orders_data_cleaned_df = db_cleaning.clean_orders_data(orders_df)
orders_data_cleaned_df.to_csv('orders_data_cleaned.csv')

db_connector.upload_to_db(orders_data_cleaned_df, 'orders_table')

url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

events_data = db_extractor.extract_JSON_file(url)
events_data.to_csv('events_data.csv')


events_df = pd.read_csv('events_data.csv')
events_df_cleaned = db_cleaning.clean_events_data(events_df)
events_df_cleaned.to_csv('events_data_cleaned.csv')

db_connector.upload_to_db(events_df_cleaned, 'dim_date_times')
