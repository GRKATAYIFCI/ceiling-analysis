import pandas as pd
from loguru import logger
import pandas.errors
import static_methods


class FirstDayChecker:
    def __init__(self, PATH, previous_day_closing_price):
        self.df = self.get_df(PATH)
        self.previous_day_closing_price = float(previous_day_closing_price)
        self.tick_size = static_methods.calculate_tick_size(self.previous_day_closing_price)
        self.ceiling_price = self._calculate_max_price()
        self.stop_price = self._calculate_stop_loss()
        #logger.info(self.previous_day_closing_price)
        #logger.info(self.ceiling_price)
        #logger.info(self._calculate_stop_loss())
    
    def get_df(self, PATH):
        df = static_methods.read_csv(PATH)
        df = static_methods.shrink_df_to_only_tradeable_times(df)
        return df
    def _calculate_max_price(self) -> float:
        scale_factor = 100
        price_in_cents = int(
            self.previous_day_closing_price * scale_factor)  #self.df.loc[self.df['recent_message_type'] == 'E'].iloc[1]['ask_price'])
        tick_size_in_cents = int(self.tick_size * scale_factor)

        ceiling_price = price_in_cents * 1.1 // tick_size_in_cents

        return ceiling_price * tick_size_in_cents / scale_factor

    def _calculate_stop_loss(self, stop_loss_rate=0.03) -> float:
        scale_factor = 100
        tick_size_in_cents = int(self.tick_size * scale_factor)
        ceiling_price_in_cents = int(self.ceiling_price * scale_factor)
        stop_price = ceiling_price_in_cents * (1 - stop_loss_rate) // tick_size_in_cents

        return stop_price * tick_size_in_cents / scale_factor

    def _find_closing_price_of_the_day(self):
        try:
            closing_price = self.df.loc[self.df['recent_message_type'] == 'E'].iloc[-1].loc['ask_price']
            return closing_price
        except IndexError:
            #print(self.df.loc[self.df['recent_message_type'] == 'E'])
            logger.error(len(self.df))
            logger.error('there is no executed orders')

    def check(self) -> float | int:
        """
        This function determines the status of position based on the relationship between stop loss and ceiling prices.

        Returns:
            float: The stop loss price if the ceiling is hit and the price returns to the stop loss price.
            int: 0.0 if the ceiling is hit but the price closes between the stop loss and ceiling (indicating a non-closed position).
            int: -1.0 if the ceiling is not hit therefore position not opened.
        """
        logger.critical(f"calculated ceiling price {self.ceiling_price}")
        if self.df.shape[0] < 20:
            return -3, self.ceiling_price, 0, 0, 0

        day_close_price = self._find_closing_price_of_the_day()
        self.df['ask_price'] = pd.to_numeric(self.df['ask_price'], errors='coerce')
        self.df['bid_price'] = pd.to_numeric(self.df['bid_price'], errors='coerce')

        ceiling_df = self.df.loc[(self.df['ask_price'] >= self.ceiling_price) & (self.df['bid_price'] >= self.stop_price)]# | ((self.df['bid_price'] != 0 ) & (self.df['ask_price'] == 0))]
        #logger.info(f"current max ask price {self.df['ask_price'].max()}")
        #logger.info(f"prev day close {self.previous_day_closing_price}")
        if not ceiling_df.empty:

            index = ceiling_df.index[0]
            stop_loss_df = self.df.loc[index:]
            stop_loss_df = stop_loss_df.loc[(self.df['bid_price'] <= self.stop_price)]
            if not stop_loss_df.empty:
                return stop_loss_df.iloc[0]['bid_price'], self.ceiling_price, day_close_price, index, stop_loss_df.index[0]

            return 0, self.ceiling_price, day_close_price, index, 0
        return -1, self.ceiling_price, 0, 0, 0


def main():
    PATH = '/home/gkatayifci/Desktop/csv_files/20231201_AKBNK.csv'
    result = FirstDayChecker(PATH, 33.43).check()  # 33.43
    logger.error(result)


if __name__ == '__main__':
    main()
