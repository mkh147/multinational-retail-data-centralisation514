import pandas as pd
import tabula
import requests
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Initialize the DatabaseConnector for the local database
    db_connector = DatabaseConnector(db_creds='db_creds.yml')

    # Initialize the DatabaseConnector for the RDS database
    rds_db_connector = DatabaseConnector(db_creds='rds_db_creds.yml')

    # Initialize the DataExtractor
    data_extractor = DataExtractor(db_connector)
    rds_data_extractor = DataExtractor(rds_db_connector)

    # Initialize the DataCleaning
    data_cleaning = DataCleaning()

    # --- Extract and clean user data from the database ---
    try:
        rds_tables = rds_db_connector.list_db_tables()
        user_data_table = next((table for table in rds_tables if 'user' in table.lower()), None)

        if user_data_table:
            print("Table containing user data:", user_data_table)
            user_data_df = rds_data_extractor.read_rds_table(user_data_table)
            cleaned_user_data_df = data_cleaning.clean_user_data(user_data_df)
            db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')
            print(cleaned_user_data_df)
        else:
            print('No table containing user data found.')
    except Exception as e:
        print(f"Error processing user data: {e}")

    # --- Extract and clean card data from a PDF ---
    try:
        pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        card_data_df = data_extractor.retrieve_pdf_data(pdf_link)
        print('Extracted Data:')
        print(card_data_df)

        if not card_data_df.empty:
            cleaned_card_data = data_cleaning.clean_card_data(card_data_df)
            print("Cleaned Data:", cleaned_card_data)
            db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')
            print("Cleaned card data has been uploaded to the 'dim_card_details' table in the 'sales_data' database.")
        else:
            print("No data extracted from the PDF.")
    except Exception as e:
        print(f"Error processing card data: {e}")

    # --- Extract and clean store data from the API ---
    try:
        store_details_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
        all_store_data = data_extractor.retrieve_stores_data(store_details_endpoint)
        
        all_store_data.to_csv('extracted_store_data.csv', index=False)
        print("Extracted Store Data:")
        print(all_store_data)

        cleaned_store_data = data_cleaning.clean_store_data(all_store_data)
        cleaned_store_data.to_csv('cleaned_store_data.csv', index=False)
        print("Cleaned Store Data:")
        print(cleaned_store_data)

        cleaned_store_data.reset_index(drop=True, inplace=True)
        if 'index' in cleaned_store_data.columns:
            cleaned_store_data.drop(columns=['index'], inplace=True)

        print("DataFrame columns before uploading:")
        print(cleaned_store_data.columns)

        db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')
        print("Cleaned store data has been uploaded to the 'dim_store_details' table in the 'sales_data' database.")
    except Exception as e:
        print(f"Error processing store data: {e}")

    # --- Extract and clean product data from S3 ---
    try:
        s3_address = 's3://data-handling-public/products.csv'
        products_data_df = data_extractor.extract_from_s3(s3_address)
        print("Extracted Products Data:")
        print(products_data_df)

        products_data_df['product_price'] = products_data_df['product_price'].astype(str)
        products_data_df['product_price_clean'] = products_data_df['product_price'].str.replace('£', '').str.replace(',', '')
        products_data_df['product_price_numeric_check'] = products_data_df['product_price_clean'].apply(lambda x: x.replace('.', '', 1).isdigit())
        non_numeric_prices = products_data_df[~products_data_df['product_price_numeric_check']]
        print("Problematic Rows:")
        print(non_numeric_prices)
        products_data_df = products_data_df[products_data_df['product_price_numeric_check']]

        if not products_data_df.empty:
            cleaned_products_data = data_cleaning.clean_products_data(products_data_df)
            print("Cleaned Products Data:")
            print(cleaned_products_data)
            db_connector.upload_to_db(cleaned_products_data, 'dim_products')
            print("Cleaned products data has been uploaded to the 'dim_products' table in the 'sales_data' database.")
        else:
            print("No product data extracted from S3.")
    except Exception as e:
        print(f"Error processing product data: {e}")

    # --- Extract and clean orders data from RDS database ---
    try:
        rds_tables = rds_db_connector.list_db_tables()
        orders_table = 'orders_table'

        if orders_table in rds_tables:
            print("Table containing orders data:", orders_table)
            orders_df = rds_data_extractor.read_rds_table(orders_table)
            print("Extracted Orders Data:")
            print(orders_df)

            cleaned_orders_df = data_cleaning.clean_orders_data(orders_df)
            print("Cleaned Orders Data:")
            print(cleaned_orders_df)

            db_connector.upload_to_db(cleaned_orders_df, 'orders_table')
            print("Cleaned orders data has been uploaded to the 'orders_table'.")
        else:
            print(f'Table {orders_table} not found in RDS database.')
    except Exception as e:
        print(f"Error processing orders data: {e}")

    # --- Extract and clean date data from JSON ---
    try:
        json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        date_data_df = data_extractor.extract_json_data(json_url)
        print("Extracted Date Data:")
        print(date_data_df)

        print("Columns in the extracted date data:")
        print(date_data_df.columns)

        if not date_data_df.empty:
            cleaned_date_data = data_cleaning.clean_date_data(date_data_df)
            print("Cleaned Date Data:")
            print(cleaned_date_data)
            db_connector.upload_to_db(cleaned_date_data, 'dim_date_times')
            print("Cleaned date data has been uploaded to the 'dim_date_times' table in the 'sales_data' database.")
        else:
            print("No date data extracted from JSON.")
    except Exception as e:
        print(f"Error processing date data: {e}")

if __name__ == "__main__":
    main()