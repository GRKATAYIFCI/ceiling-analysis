import pandas as pd
import static_methods
from loguru import logger

class PreviousDayPropertyGetter:
    def __init__(self, PATH):
        self.PATH = PATH
        self.df = static_methods.read_csv(self.PATH)
        self._find_execution_properties()

    def find_prev_day_closing_price(self):
        logger.info(f" execution order # {self.df.loc[self.df['recent_message_type'] == 'E'].shape[0]}")
        if self.df.loc[self.df['recent_message_type'] == 'E'].shape[0] > 2:
            index = self.df.loc[self.df['recent_message_type'] == 'E'].index[-1]
#            if filtered_df.iloc[-1]['ask_price'] == 0:
#                return filtered_df.iloc[-1]['bid_price']
            logger.warning(self.df.loc[index, 'execution_price'])
            return self.df.loc[index, 'execution_price']
        else:
            logger.debug(self.df)
            return 0
        
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

    def _get_executed_price_column(self, row):
        if row['recent_message_type'] == 'E':
            if row['bid_quantity_diff'] != 0.0:
                return row['prev_bid_price'] if row['recent_message_type'] == 'E' else None
            else:
                return row['prev_ask_price'] if row['recent_message_type'] == 'E' else None
        else:
            return None
