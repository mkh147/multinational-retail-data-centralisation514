import pandas as pd
import numpy as np
import re

class DataCleaning:
    def clean_user_data(self, user_data_df):
        """
        Clean the user data DataFrame by removing rows with NULL values and converting data types.

        Parameters:
        user_data_df (pandas.DataFrame): DataFrame containing the user data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame.
        """
        # Replace 'NULL' string with actual None
        user_data_df.replace('NULL', pd.NA, inplace=True)

        # Remove rows with NULL values
        user_data_df = user_data_df.dropna(subset=['first_name', 'last_name', 'date_of_birth', 'country_code', 'user_uuid', 'join_date'])

        # Remove rows with numeric characters in first_name and last_name
        user_data_df = user_data_df[~user_data_df['first_name'].str.contains(r'\d', na=False)]
        user_data_df = user_data_df[~user_data_df['last_name'].str.contains(r'\d', na=False)]

        # Convert 'date_of_birth' and 'join_date' to datetime
        user_data_df['date_of_birth'] = pd.to_datetime(user_data_df['date_of_birth'], errors='coerce')
        user_data_df['join_date'] = pd.to_datetime(user_data_df['join_date'], errors='coerce')

        # Ensure 'first_name', 'last_name', and 'country_code' are strings
        user_data_df['first_name'] = user_data_df['first_name'].astype(str)
        user_data_df['last_name'] = user_data_df['last_name'].astype(str)
        user_data_df['country_code'] = user_data_df['country_code'].astype(str)

        # Remove rows with invalid UUIDs
        valid_uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        user_data_df = user_data_df[user_data_df['user_uuid'].str.match(valid_uuid_pattern, na=False)]

        # Drop the index column if it exists
        if 'index' in user_data_df.columns:
            user_data_df = user_data_df.drop(columns=['index'])

        return user_data_df

    def clean_card_data(self, card_data_df):
        """
        Clean the card data DataFrame by removing rows with NULL values and converting data types.

        Parameters:
        card_data_df (pandas.DataFrame): DataFrame containing the card data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame.
        """
        # Remove rows with NULL values
        card_data_df = card_data_df.dropna(subset=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed'])

        # Remove rows with duplicated card numbers
        card_data_df = card_data_df.drop_duplicates(subset=['card_number'])

        # Convert expiry_date to a consistent format (e.g., MM/YY)
        card_data_df['expiry_date'] = pd.to_datetime(card_data_df['expiry_date'], format='%m/%y', errors='coerce').dt.strftime('%m/%y')

        # Convert date_payment_confirmed to datetime
        card_data_df['date_payment_confirmed'] = pd.to_datetime(card_data_df['date_payment_confirmed'], errors='coerce')

        # Ensure card_number is treated as a string
        card_data_df['card_number'] = card_data_df['card_number'].astype(str)

        return card_data_df

    def clean_date_data(self, date_data_df):
        """
        Clean the date data DataFrame by removing rows with NULL values and converting data types.

        Parameters:
        date_data_df (pandas.DataFrame): DataFrame containing the date data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame.
        """
        # Remove rows with NULL values
        date_data_df = date_data_df.dropna(subset=['timestamp', 'month', 'year', 'day', 'time_period'])

        # Convert timestamp to datetime
        date_data_df['timestamp'] = pd.to_datetime(date_data_df['timestamp'], errors='coerce')

        # Ensure year, month, day, and time_period are strings
        date_data_df['year'] = date_data_df['year'].astype(str)
        date_data_df['month'] = date_data_df['month'].astype(str)
        date_data_df['day'] = date_data_df['day'].astype(str)
        date_data_df['time_period'] = date_data_df['time_period'].astype(str)

        # Remove rows where length of 'year', 'month', 'day', or 'time_period' exceeds the expected length
        date_data_df = date_data_df[
            (date_data_df['year'].str.len() <= 4) &
            (date_data_df['month'].str.len() <= 10) &
            (date_data_df['day'].str.len() <= 2) &
            (date_data_df['time_period'].str.len() <= 20)
        ]

        return date_data_df

    def convert_product_weights(self, products_data_df):
        """
        Convert different weight units to kg.

        Parameters:
        products_data_df (pandas.DataFrame): DataFrame containing the products data.

        Returns:
        pandas.DataFrame: DataFrame with weights converted to kg.
        """
        def convert_weight_to_kg(weight):
            weight = str(weight).lower().strip()
            if 'x' in weight:
                parts = weight.split('x')
                if len(parts) == 2:
                    try:
                        number_of_units = float(parts[0].strip())
                        unit_weight = parts[1].strip()
                        if 'kg' in unit_weight:
                            return number_of_units * float(unit_weight.replace('kg', '').strip())
                        elif 'g' in unit_weight:
                            return number_of_units * float(unit_weight.replace('g', '').strip()) / 1000
                        elif 'ml' in unit_weight:
                            return number_of_units * float(unit_weight.replace('ml', '').strip()) / 1000
                        elif 'l' in unit_weight:
                            return number_of_units * float(unit_weight.replace('l', '').strip())
                    except:
                        return np.nan
            else:
                try:
                    if 'kg' in weight:
                        return float(weight.replace('kg', '').strip())
                    elif 'g' in weight:
                        return float(weight.replace('g', '').strip()) / 1000
                    elif 'ml' in weight:
                        return float(weight.replace('ml', '').strip()) / 1000
                    elif 'l' in weight:
                        return float(weight.replace('l', '').strip())
                    else:
                        return float(weight)
                except ValueError:
                    return np.nan

        products_data_df['weight'] = products_data_df['weight'].apply(convert_weight_to_kg)
        return products_data_df

    def clean_products_data(self, products_data_df):
        """
        Clean the products data DataFrame by removing erroneous values, handling special cases in weights, and formatting errors.

        Parameters:
        products_data_df (pandas.DataFrame): DataFrame containing the products data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame.
        """
        # Ensure product_price is treated as a string
        products_data_df['product_price'] = products_data_df['product_price'].astype(str)

        # Remove £ symbol and check for numeric values in product_price
        products_data_df['product_price_clean'] = products_data_df['product_price'].str.replace('£', '').str.replace(',', '')
        products_data_df['numeric_check'] = products_data_df['product_price_clean'].apply(lambda x: x.replace('.', '', 1).isdigit())
        products_data_df = products_data_df[products_data_df['numeric_check']]

        # Handle weights like '12 x 100g' and convert them to a single value in kg
        def parse_weight(weight):
            if 'x' in weight:
                parts = weight.split('x')
                if len(parts) == 2:
                    try:
                        total_weight = float(parts[0].strip()) * float(re.findall(r'\d+', parts[1])[0].strip()) / 1000
                        return total_weight
                    except:
                        return None
            else:
                try:
                    return float(re.findall(r'\d+', weight)[0].strip()) / 1000
                except:
                    return None

        products_data_df['weight'] = products_data_df['weight'].apply(parse_weight)

        # Remove rows with NULL values after weight conversion
        products_data_df = products_data_df.dropna(subset=['weight'])

        # Convert date_added to datetime
        products_data_df['date_added'] = pd.to_datetime(products_data_df['date_added'], errors='coerce')

        # Ensure product_code and EAN are treated as strings
        products_data_df['product_code'] = products_data_df['product_code'].astype(str)
        products_data_df['EAN'] = products_data_df['EAN'].astype(str)

        # Add weight_class column
        products_data_df['weight_class'] = np.where(products_data_df['weight'] < 2, 'Light',
                                                    np.where(products_data_df['weight'] < 40, 'Mid_Sized',
                                                             np.where(products_data_df['weight'] < 140, 'Heavy', 'Truck_Required')))

        # Drop the temporary columns used for checking numeric prices
        products_data_df = products_data_df.drop(columns=['numeric_check', 'product_price_clean'])

        return products_data_df

    def clean_store_data(self, stores_data_df):
        """
        Clean the store data DataFrame by removing rows with NULL values and converting data types.

        Parameters:
        stores_data_df (pandas.DataFrame): DataFrame containing the store data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame.
        """
        # Remove rows with any NULL values
        stores_data_df = stores_data_df.dropna(subset=['longitude', 'latitude', 'store_code', 'opening_date', 'store_type'])

        # Convert 'opening_date' to datetime
        stores_data_df['opening_date'] = pd.to_datetime(stores_data_df['opening_date'], errors='coerce')

        # Remove rows where 'opening_date' conversion resulted in NaT
        stores_data_df = stores_data_df.dropna(subset=['opening_date'])

        # Convert longitude and latitude to float
        stores_data_df['longitude'] = pd.to_numeric(stores_data_df['longitude'], errors='coerce')
        stores_data_df['latitude'] = pd.to_numeric(stores_data_df['latitude'], errors='coerce')

        # Ensure 'store_code' is treated as a string
        stores_data_df['store_code'] = stores_data_df['store_code'].astype(str)

        # Remove rows with non-alphanumeric 'store_type'
        stores_data_df = stores_data_df[stores_data_df['store_type'].str.isalnum()]

        # Remove rows with duplicate 'store_code' values
        stores_data_df = stores_data_df.drop_duplicates(subset=['store_code'])

        # Remove alphabetic characters from 'staff_numbers'
        stores_data_df['staff_numbers'] = stores_data_df['staff_numbers'].apply(lambda x: re.sub(r'\D', '', x))

        # Convert 'staff_numbers' to numeric
        stores_data_df['staff_numbers'] = pd.to_numeric(stores_data_df['staff_numbers'], errors='coerce')

        return stores_data_df

    def clean_orders_data(self, orders_data_df):
        """
        Clean the orders data DataFrame by removing unnecessary columns and rows with NULL values.

        Parameters:
        orders_data_df (pandas.DataFrame): DataFrame containing the orders data.

        Returns:
        pandas.DataFrame: Cleaned DataFrame.
        """
        # Remove unnecessary columns
        orders_data_df = orders_data_df.drop(columns=['first_name', 'last_name', '1', 'level_0', 'index'], errors='ignore')

        # Remove rows with NULL values in critical columns
        orders_data_df = orders_data_df.dropna(subset=['date_uuid', 'user_uuid', 'card_number', 'store_code', 'product_code', 'product_quantity'])

        # Convert 'date_uuid' and 'user_uuid' to string (if they aren't already)
        orders_data_df['date_uuid'] = orders_data_df['date_uuid'].astype(str)
        orders_data_df['user_uuid'] = orders_data_df['user_uuid'].astype(str)

        # Ensure 'card_number', 'store_code', and 'product_code' are strings
        orders_data_df['card_number'] = orders_data_df['card_number'].astype(str)
        orders_data_df['store_code'] = orders_data_df['store_code'].astype(str)
        orders_data_df['product_code'] = orders_data_df['product_code'].astype(str)

        # Convert 'product_quantity' to integer
        orders_data_df['product_quantity'] = pd.to_numeric(orders_data_df['product_quantity'], errors='coerce').astype('Int64')

        return orders_data_df