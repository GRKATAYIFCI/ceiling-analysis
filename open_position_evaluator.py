from loguru import logger
import pandas as pd

import static_methods
import re

class OpenPositionEvaluator:
    def __init__(self, PATH, previous_day_closing_price, entry_price, stop_loss_percentage,
                 profit_percentage):
        self.df = static_methods.read_csv(PATH)
        self.entry_price = entry_price
        self.previous_day_closing_price = previous_day_closing_price
        self.stop_loss_perc = stop_loss_percentage
        self.profit_perc = profit_percentage
        self.last_executed_price: float = 0


    def _get_executed_price_column(self, row):
        if row['recent_message_type'] == 'E':
            if row['bid_quantity_diff'] != 0.0:
                return row['prev_bid_price'] if row['prev_message_type'] == 'E' else None
            else:
                return -row['prev_ask_price'] if row['prev_message_type'] == 'E' else None
        else:
            return None

    def _add_percentages(self):
        try:
            self.df['execution_percentage'] = (abs(
                self.df['execution_price']) - self.entry_price) / self.entry_price
        except TypeError:
            self.df

    def _find_first_out_of_bounds(self):
        #if self.df.loc[self.df['recent_message_type'] == 'E'].shape[0] <= 2:
        #    return 0, 0
        #for index, row in self.df.loc[pd.notna(self.df['execution_percentage'])].iterrows():
        #    if not (-self.stop_loss_perc <= row['execution_percentage'] <= self.profit_perc):
        #        return index, abs(row['execution_price'])

        #valid_rows = self.df.dropna(subset=['execution_price'])
        #last_execution_price = abs(valid_rows.iloc[-1]['execution_price']) if not valid_rows.empty else None

        # Return the length of the DataFrame and the last valid 'execution_price'

        for index, row in self.df.loc[self.df['recent_message_type'] != 'O'].iterrows():
            if row['bid_price'] == 0 and row['bid_qty'] == 0:
                continue

            current_percentage = (row['bid_price'] - self.entry_price) / self.entry_price

            if not (-self.stop_loss_perc <= current_percentage <= self.profit_perc):
                return index, row['bid_price']

        return len(self.df), self.df.iloc[-1]['bid_price']

    def _find_execution_properties(self):
        self.df['prev_bid_price'] = self.df['bid_price'].shift(1)
        self.df['prev_bid_quantity'] = self.df['bid_qty'].shift(1)
        self.df['prev_message_type'] = self.df['recent_message_type'].shift(1)

        self.df['bid_quantity_diff'] = self.df.apply(
            lambda x: x['bid_qty'] - x['prev_bid_quantity']
            if x['bid_price'] == x['prev_bid_price']
            else x['prev_bid_quantity'], axis=1)

        self.df['prev_ask_price'] = self.df['ask_price'].shift(1)
        self.df['prev_ask_quantity'] = self.df['ask_qty'].shift(1)

        self.df['ask_quantity_diff'] = self.df.apply(
            lambda x: x['ask_qty'] - x['prev_ask_quantity']
            if x['ask_price'] == x['prev_ask_price']
            else x['prev_ask_quantity'], axis=1)

        self.df['execution_price'] = self.df.apply(self._get_executed_price_column, axis=1)

        self.df.drop(
            columns=['prev_bid_price', 'prev_bid_quantity', 'prev_ask_quantity', 'prev_ask_price', 'prev_message_type'],
            inplace=True
        )
    

    def runner(self):
        """
        tracks position cycle
        :return:
        closing price
        """
        pattern = re.compile(r'\.R$')
        if pattern.search(self.df.iloc[-1]['security_name']):
            logger.info('THIS IS RUCHAN SECURITY')
            return False

        self.df = static_methods.shrink_df_to_only_tradeable_times(self.df)

                   
        if self.df.shape[0] == 1:
            logger.error('Critical Error!')
            return 0
        #self._find_execution_properties()
        #self._add_percentages()
        index, price = self._find_first_out_of_bounds()
        logger.error(f"exit price {price}")
        logger.warning(f"entry price {self.entry_price}")
        logger.error(index)
        return price

def main():
    PATH = '/home/gkatayifci/market-data/1_BIST/3_Tob_Change/2024/202402/eq/20240201_EQU_AKSUE.csv'
    tob_ceiling_analysis_obj = OpenPositionEvaluator(PATH, 32.8, 33.2, 0.03, 0.05)
    tob_ceiling_analysis_obj.runner()


if __name__ == '__main__':
    main()