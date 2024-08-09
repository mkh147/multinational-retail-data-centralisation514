import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_rds_table(self, table_name, engine):
        return pd.read_sql_table(table_name, engine)  
    
    def retrieve_pdf_data(self, link):
        dataframe = tabula.read_pdf(link, pages='all')
        dataframe = pd.concat(dataframe)
        return dataframe

    def list_number_of_stores(self, number_of_stores_endpoint, api_key):
        headers = {
            "x-api-key": api_key
        }
        response = requests.get(number_of_stores_endpoint, headers=headers)
        if response.status_code == 200:
            number = response.json()
            return number['number_stores'] 
        else:
            response.raise_for_status()
    

    def retrieve_stores_data(self, store_details_endpoint, api_key, number_of_stores):
        headers = {
            "x-api-key": api_key
        }
        store_data_list = []
        for store_number in range(0, number_of_stores):
            url = store_details_endpoint.format(store_number=store_number)
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                store_data = response.json()
                store_data_list.append(store_data)
            else:
                print(f"Failed to retrieve data for store {store_number}")

        store_data_list = pd.DataFrame(store_data_list)
        return store_data_list
    
    def extract_from_s3(self, s3_address):
        # Parse the S3 address
        s3_parts = s3_address.replace("s3://", "").split('/', 1)
        bucket_name = s3_parts[0]
        file_key = s3_parts[1]

        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])
        
        return df
    
    def extract_JSON_file(self, url):
        response = requests.get(url)
        data = response.json()
        data = pd.DataFrame.from_dict(data)


        return data