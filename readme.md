# Multinational Retail Data Centralisation Project

This project focuses on extracting and cleaning sales data from various sources such as databases, APIs, PDFs, and S3 buckets. The cleaned data is then uploaded to a PostgreSQL database called 'sales-data'.

# Table of Contents

1.0 [Prerequisites](#prerequisites)  
2.0 [Project Structure](#project-structure)  
3.0 [Usage](#usage)  
4.0 [Data Cleaning Methods](#data-cleaning-methods)  
5.0 [Database Schema](#database-schema)  
6.0 [Notes](#notes)

## 1.0 Prerequisites

- Python 3.x
- PostgreSQL
- Anaconda
- AWS CLI

### 1.1 Python Packages Installation

First, create a new conda environment:

    conda create --name data_env python=3.9
    conda activate data_env

Install the required Python packages using conda and pip:

    conda install pip pandas requests sqlalchemy psycopg2
    pip install tabula-py JPype1 pyyaml boto3

### 1.2 AWS CLI Installation

**For macOS**

Install AWS CLI using Homebrew, and configure AWS CLI with your credentials:

    brew install awscli
    awscli configure

**For Windows**

Install AWS CLI using the MSI installer from the [AWS CLI website](https://aws.amazon.com/cli/), and configure your credentials:

    msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi
    awscli configure

### 1.3 PostgreSQL database

Ensure that you have configured a PostgreSQL database and create a YAML file called db_creds.yml with the necessary credentials.

Ensure that you have access to the S3 buckets and API endpoints used in the project.

## 2.0 Project Structure

### 2.1 Modules

'**database_utils.py**': Contains the '**DatabaseConnector**' class for database operations.
'**data_extraction.py**': Contains the '**DataExtractor**' class for extracting data from various sources.
'**data_cleaning.py**': Contains the '**DataCleaning**' class for cleaning the extracted data.
'**main.py**': The main script that orchestrates the data extraction, cleaning, and uploading process.

## 3.0 Usage

Update the paths and credentials in the db_creds.yml and rds_db_crfile.
Ensure thatt all of the packages installed are up to date.

Run the main script:

    python3 main.py

The script will:

Extract and clean user data from the database.
Extract and clean card data from a PDF.
Extract and clean store data from an API.
Extract and clean product data from an S3 bucket.
Extract and clean orders data from the database.
Extract and clean date data from a JSON file on S3.
Upload the cleaned data to the specified tables in the PostgreSQL database.

## 4.0 Data Cleaning Methods

### 4.1 User Data Cleaning

- Remove rows with NULL values.
- Remove rows with numeric characters in `first_name` and `last_name`.
- Convert `date_of_birth` and `join_date` to datetime.
- Ensure `first_name`, `last_name`, and `country_code` are strings.
- Remove rows with invalid UUIDs.

### 4.2 Card Data Cleaning

- Remove rows with NULL values.
- Remove rows with duplicated card numbers.
- Convert `expiry_date` to a consistent format.
- Convert `date_payment_confirmed` to datetime.

### 4.3 Date Data Cleaning

- Remove rows with NULL values.
- Convert `timestamp` to datetime.
- Ensure `year`, `month`, `day`, and `time_period` are strings.
- Remove rows where the length of `year`, `month`, `day`, or `time_period` exceeds the expected length.

### 4.4 Product Data Cleaning

- Ensure `product_price` is treated as a string.
- Handle weights like '12 x 100g' and convert them to a single value in kg.
- Remove rows with NULL values after weight conversion.
- Convert `date_added` to datetime.
- Ensure `product_code` and `EAN` are treated as strings.
- Add `weight_class` column.

### 4.5 Store Data Cleaning

- Remove rows with any NULL values.
- Convert `opening_date` to datetime.
- Convert `longitude` and `latitude` to float.
- Ensure `store_code` is treated as a string.
- Remove rows with non-alphanumeric `store_type`.
- Remove rows with duplicate `store_code` values.
- Remove alphabetic characters from `staff_numbers`.

### 4.6 Orders Data Cleaning

- Remove unnecessary columns.
- Remove rows with NULL values in critical columns.
- Convert `date_uuid` and `user_uuid` to string.
- Ensure `card_number`, `store_code`, and `product_code` are strings.
- Convert `product_quantity` to integer.

## 5.0 Database Schema

The database schema follows a star-based schema with dimension tables (`dim_users`, `dim_card_details`, `dim_store_details`, `dim_products`, `dim_date_times`) and a fact table (`orders_table`).

## 6.0 Notes

- Ensure that the PostgreSQL database is running and accessible.
- Ensure that you have the necessary permissions to access the S3 buckets and API endpoints.
- Make sure to replace placeholder values in the YAML file with your actual credentials.