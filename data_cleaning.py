import pandas as pd

class DataCleaning:
    def __init__(self, df) -> None:
        self.df = df

    def clean_user_data(self):
        self.drop_index_column()
        self.sort_country_code_column()
        self.sort_country_column()
        self.sort_date_format()
        return self.df

    def drop_index_column(self):
        self.df = self.df.drop(columns=['index'])
        return self.df

    def sort_country_code_column(self):
        self.df = self.df.replace('GGB', 'GB')
        self.df = self.df.replace('NULL', None)
        self.df['country_code'] = self.df['country_code'].where(self.df['country_code'].isin(['DE','US', 'GB']), None)
        return self.df
    
    def sort_country_column(self):
        self.df['country'] = self.df['country'].where(self.df['country'].isin(['Germany','United States', 'United Kingdom']), None)
        return self.df
    
    def sort_date_format(self):
        self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth'], errors='coerce')
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], errors='coerce')
        return self.df


   