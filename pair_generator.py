import pandas as pd
from loguru import logger

class PairGenerator:
    def __init__(self, PATH, start_date=20201001, end_date=20240430):
        self.start_date = start_date
        self.end_date = end_date
        self.ceiling_df = self._read_ceiling_day_csv(PATH)

    def _read_ceiling_day_csv(self, PATH):
        self.ceiling_df = pd.read_csv(PATH)
        try:
            self.ceiling_df.drop(columns=['close_date', 'open_price_in_range', 'opened_next_day_at', 'rates', 'first_day_pnl',
                             'close_percentage', ], inplace=True)
            self.ceiling_df = self.ceiling_df.loc[(self.ceiling_df['date'] >= self.start_date) & (self.ceiling_df['date'] <= self.end_date)]

        except:
            self.ceiling_df = pd.read_csv(PATH, header=None)
            self.ceiling_df.columns = ['date', 'security_name']
            logger.info('This is spesific pair generation operation')
        return self.ceiling_df

    def ana_pazar_filter(self):
        logger.info(self.ceiling_df)
        file = open('yildiz.txt', 'r')
        content = [line.split(' ')[0] for line in file.readlines()]
        file.close()
        indicies = []
        for index, row in self.ceiling_df.iterrows():
            if row['security_name'].split('.')[0]  in content:
                indicies.append(index)
        self.ceiling_df.drop(index=indicies, inplace=True)

    def yildiz_pazar_filter(self):
        logger.info(self.ceiling_df)
        file = open('yildiz.txt', 'r')
        content = [line.split(' ')[0] for line in file.readlines()]
        file.close()
        indicies = []
        for index, row in self.ceiling_df.iterrows():
            if row['security_name'].split('.')[0] not in content:
                indicies.append(index)
        self.ceiling_df.drop(index=indicies, inplace=True)

    def _merge_columns(self, row):
        base = '/home/gkatayifci/market-data/1_BIST/3_Tob_Change/'
        previous_day_formatted_date = row['previous_day_formatted_date']
        next_formatted_date = row['next_day_formatted_date']
        formatted_date = row['formatted_date']
        
        sec_name = row['security_name']
        
        year = row['formatted_date'] // 10000
        year_month = row['formatted_date'] // 100

        next_year = row['next_day_formatted_date'] // 10000
        next_year_month = row['next_day_formatted_date'] // 100

        prev_year = row['previous_day_formatted_date'] // 10000
        prev_year_month = row['previous_day_formatted_date'] // 100

        previous_day_formatted_col = f'{base}{prev_year}/{prev_year_month}/eq/{previous_day_formatted_date}_EQU_{sec_name}' 
        next_day_formatted_col = f'{base}{next_year}/{next_year_month}/eq/{next_formatted_date}_EQU_{sec_name}'
        formatted_col = f'{base}{year}/{year_month}/eq/{formatted_date}_EQU_{sec_name}'

        previous_day_formatted_v2 = f'{base}{prev_year}/{prev_year_month}/eq/{previous_day_formatted_date}_{sec_name}'
        next_day_formatted_v2 = f'{base}{next_year}/{next_year_month}/eq/{next_formatted_date}_{sec_name}'
        formatted_col_v2 = f'{base}{year}/{year_month}/eq/{formatted_date}_{sec_name}'

        return pd.Series(data=[row['date'], (previous_day_formatted_col, previous_day_formatted_v2),(formatted_col, formatted_col_v2), (next_day_formatted_col,next_day_formatted_v2)],
                        index=['date', 'previous_day_directory', 'directory','next_day_directory'])


    def generate_path_pairs(self) -> pd.DataFrame:
        # self.ana_pazar_filter()
        self.ceiling_df['formatted_date'] = pd.to_datetime(self.ceiling_df['date'].astype(int).astype(str), format='%Y%m%d')
        self.ceiling_df['next_day_formatted_date'] = pd.to_datetime(self.ceiling_df['formatted_date'] + pd.tseries.offsets.BusinessDay(n = 1))
        self.ceiling_df['previous_day_formatted_date'] = pd.to_datetime(self.ceiling_df['formatted_date'] + pd.tseries.offsets.BusinessDay(n = -1))
        
        pair_df = self.ceiling_df[['date', 'security_name', 'previous_day_formatted_date', 'formatted_date', 'next_day_formatted_date']].copy()
        pair_df['formatted_date'] = pair_df['formatted_date'].dt.strftime('%Y%m%d').astype(int)
        pair_df['next_day_formatted_date'] = pair_df['next_day_formatted_date'].dt.strftime('%Y%m%d').astype(int)
        pair_df['previous_day_formatted_date'] = pair_df['previous_day_formatted_date'].dt.strftime('%Y%m%d').astype(int)
        pair_df = pair_df.apply(self._merge_columns, axis=1)
        return pair_df
    
    def generate_spesific_pairs(self):
        self.ceiling_df['security_name'] = self.ceiling_df['security_name'] + '.csv'
        
        self.ceiling_df['formatted_date'] = pd.to_datetime(self.ceiling_df['date'].astype(int).astype(str), format='%Y%m%d')
        self.ceiling_df['next_day_formatted_date'] = pd.to_datetime(self.ceiling_df['formatted_date'] + pd.tseries.offsets.BusinessDay(n = 1))
        self.ceiling_df['previous_day_formatted_date'] = pd.to_datetime(self.ceiling_df['formatted_date'] + pd.tseries.offsets.BusinessDay(n = -1))

        self.ceiling_df['formatted_date'] = self.ceiling_df['formatted_date'].dt.strftime('%Y%m%d').astype(int)
        self.ceiling_df['next_day_formatted_date'] = self.ceiling_df['next_day_formatted_date'].dt.strftime('%Y%m%d').astype(int)
        self.ceiling_df['previous_day_formatted_date'] = self.ceiling_df['previous_day_formatted_date'].dt.strftime('%Y%m%d').astype(int)
        logger.debug(self.ceiling_df.to_string())

        self.ceiling_df = self.ceiling_df.apply(self._merge_columns, axis=1)
        logger.debug(self.ceiling_df.to_string())
        return self.ceiling_df