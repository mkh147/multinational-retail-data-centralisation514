import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    """
    A class used to extract data from various sources including RDS tables, PDFs, APIs, S3, and JSON files.
    """

    def __init__(self) -> None:
        """
        Initializes the DataExtractor class.
        """
        pass

    def read_rds_table(self, table_name, engine):
        """
        Reads a table from an RDS database into a pandas DataFrame.

        Parameters:
        table_name (str): The name of the table to read.
        engine: The SQLAlchemy engine connected to the RDS instance.

        Returns:
        pd.DataFrame: The DataFrame containing the table data.
        """
        return pd.read_sql_table(table_name, engine)  
    
    def retrieve_pdf_data(self, link):
        """
        Retrieves data from a PDF file located at the specified link and combines all pages into a single DataFrame.

        Parameters:
        link (str): The URL of the PDF file to retrieve data from.

        Returns:
        pd.DataFrame: The DataFrame containing the extracted PDF data.
        """
        dataframe = tabula.read_pdf(link, pages='all')
        dataframe = pd.concat(dataframe)
        return dataframe

    def list_number_of_stores(self, number_of_stores_endpoint, api_key):
        """
        Retrieves the number of stores from an API endpoint.

        Parameters:
        number_of_stores_endpoint (str): The API endpoint to retrieve the number of stores.
        api_key (str): The API key for authentication.

        Returns:
        int: The number of stores.
        """
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
        """
        Retrieves detailed data for each store from an API and combines the data into a single DataFrame.

        Parameters:
        store_details_endpoint (str): The API endpoint to retrieve store details, with a placeholder for store numbers.
        api_key (str): The API key for authentication.
        number_of_stores (int): The number of stores to retrieve data for.

        Returns:
        pd.DataFrame: The DataFrame containing the retrieved stores data.
        """
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
        """
        Extracts a CSV file from an S3 bucket and loads it into a pandas DataFrame.

        Parameters:
        s3_address (str): The S3 address of the file to be extracted, in the format 's3://bucket_name/file_key'.

        Returns:
        pd.DataFrame: The DataFrame containing the data extracted from the S3 file.
        """
        # Parse the S3 address
        s3_parts = s3_address.replace("s3://", "").split('/', 1)
        bucket_name = s3_parts[0]
        file_key = s3_parts[1]

        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])
        
        return df
    
    def extract_JSON_file(self, url):
        """
        Extracts data from a JSON file available at a given URL and loads it into a pandas DataFrame.

        Parameters:
        url (str): The URL of the JSON file to be extracted.

        Returns:
        pd.DataFrame: The DataFrame containing the data extracted from the JSON file.
        """
        response = requests.get(url)
        data = response.json()
        data = pd.DataFrame.from_dict(data)

        return data
