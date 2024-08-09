import pandas as pd
import numpy as np

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        self.drop_index_column(df)
        self.sort_country_code_column(df)
        self.sort_country_column(df)
        self.sort_date_format(df)
        return self.df

    def drop_index_column(self,df):
        df = df.drop(columns=['index'])
        return df

    def sort_country_code_column(self,df):
        df = df.replace('GGB', 'GB')
        df = df.replace('NULL', None)
        df['country_code'] = df['country_code'].where(df['country_code'].isin(['DE','US', 'GB']), None)
        return df
    
    def sort_country_column(self,df):
        df['country'] = df['country'].where(df['country'].isin(['Germany','United States', 'United Kingdom']), None)
        return df
    
    def sort_date_format(self,df):
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        return self.df 

    def clean_card_data(self, df):
        df['card_number'] = df['card_number'].str.replace('?', '', regex=False)
        card_provider_list = ["Diners Club / Carte Blanche", "American Express", "JCB 16 digit", "JCB 15 digit", "Maestro", "Mastercard", "Discover", "VISA 19 digit", "VISA 16 digit", "VISA 13 digit" ]
        df = df[df["card_provider"].isin(card_provider_list)] 
        return df
    
    def clean_store_data(self, df):
        df.replace("", np.nan, inplace=True)
        df = df.dropna(how='all')
        Country_code_list = ["DE", "GB", "US"]
        df = df[df["country_code"].isin(Country_code_list)]
        df["staff_numbers"] = df["staff_numbers"].str.replace(r'[a-zA-Z]', '', regex=True)
        df["staff_numbers"] = pd.to_numeric(df["staff_numbers"])
        return df
    
    def clean_products_data(self, df):
        list = ["Still_avaliable", "Removed"]
        df = df[df["removed"].isin(list)]
        return df
    

    def convert_product_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        def convert_weight(weight):
            weight = str(weight).lower().replace(' ', '')
            
            # Handling formats with multiplication, e.g., '12x100g'
            if 'x' in weight:
                parts = weight.split('x')
                if len(parts) == 2:
                    try:
                        num = float(parts[0])
                        unit_weight = parts[1]
                        weight_value = ''.join([char for char in unit_weight if char.isdigit() or char == '.'])
                        weight_unit = ''.join([char for char in unit_weight if char.isalpha()])
                        if weight_unit == 'g':
                            return num * float(weight_value) / 1000
                        elif weight_unit == 'kg':
                            return num * float(weight_value)
                        elif weight_unit == 'mg':
                            return num * float(weight_value) / 1_000_000
                        elif weight_unit == 'lb':
                            return num * float(weight_value) * 0.453592
                        elif weight_unit == 'oz':
                            return num * float(weight_value) * 0.0283495
                        elif weight_unit == 'ml':
                            return num * float(weight_value) / 1000  # Assuming 1:1 ratio of ml to g
                    except ValueError:
                        return None
            
            if 'kg' in weight:
                return float(weight.replace('kg', ''))
            elif 'g' in weight:
                return float(weight.replace('g', '')) / 1000
            elif 'mg' in weight:
                return float(weight.replace('mg', '')) / 1_000_000
            elif 'lb' in weight:
                return float(weight.replace('lb', '')) * 0.453592
            elif 'oz' in weight:
                return float(weight.replace('oz', '')) * 0.0283495
            elif 'ml' in weight:
                return float(weight.replace('ml', '')) / 1000  # Assuming 1:1 ratio of ml to g
            else:
                cleaned_weight = ''.join([char for char in weight if char.isdigit() or char == '.'])
                if cleaned_weight:
                    return float(cleaned_weight) / 1000  # Assuming grams if no unit
                else:
                    return None 
                
        df.loc[:, 'weight'] = df['weight'].apply(convert_weight)
        return df
        
        
    def clean_orders_data(self, df):
        df = df.drop(columns=['first_name', 'last_name', '1', 'level_0'])
        return df
    
    def clean_events_data(self, df):
        list = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
        df = df[df["month"].isin(list)]
        return df
    