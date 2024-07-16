from loguru import logger
import pandas as pd
from pathlib import Path
import os
import time

from open_position_evaluator import OpenPositionEvaluator as evaluator
from pair_generator import PairGenerator as pair_generator
from first_day_checker import FirstDayChecker as checker 
import static_methods
from previous_day_properties import PreviousDayPropertyGetter as getter

start = time.perf_counter()
PATH = 'input/yildiz_ana_pazar_above_8_percent_days_for_after_20201001_time_series.csv'
is_checked = False
is_evaluated = False
is_spesific = True
OUTPUT_DIR = '20240126_not_entering_stop_to_be_position_spesific' 
int_start_date = 20240126
int_end_date = 20240226


def main():
    if not os.path.exists(f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}'):
        os.mkdir(f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}')

    # CHECKING
    if not is_checked:
        ceiling_pair_df = check()
    else:
        ceiling_pair_df = pd.read_csv(f'input/ceiling_directory_pairs_with_extra_columns_{int_end_date}_{int_end_date}.csv')
    logger.info(ceiling_pair_df.loc[ceiling_pair_df['closed_in_first_day'] == True])
    
    print('# stopped position', len(ceiling_pair_df.loc[(ceiling_pair_df['closed_at'] < ceiling_pair_df['opened_at'])]))
    print('# of hidden stopped positions', len(ceiling_pair_df.loc[ceiling_pair_df['closed_at'] < ceiling_pair_df['day_closed_at']]))

    # EVALUATION
    if not is_evaluated:
        ceiling_pair_df = evaluate(ceiling_pair_df)
    else:
        ceiling_pair_df = pd.read_csv(f'input/ceiling_directory_pairs_with_extra_columns_full_of_values_{int_start_date}_{int_end_date}.csv')

    # REPORTING
    ceiling_pair_df['pnl_as_ratio'] = ceiling_pair_df.apply(lambda row: (row['closed_at'] - row['opened_at']) / row['opened_at'], axis=1)
    
    filtered_df = ceiling_pair_df.loc[(ceiling_pair_df['pnl_as_ratio'] >= -0.23) & (ceiling_pair_df['pnl_as_ratio'] <= 0.1)]
    error_df = ceiling_pair_df.loc[(ceiling_pair_df['pnl_as_ratio'] <= -0.23) & (ceiling_pair_df['pnl_as_ratio'] >= 0.1)]
    abnormal_positive = ceiling_pair_df.loc[(ceiling_pair_df['pnl_as_ratio'] >= 0.1)]
    abnormal_negative = ceiling_pair_df.loc[ceiling_pair_df['pnl_as_ratio'] <= -0.06]  

    day_separated_time_series_df, stats_df = _calculate_statistic(filtered_df)
    time_series_df, pnl_trade_series, pnl_day_series, pnl_as_ratio = _generate_series(filtered_df, day_separated_time_series_df)
    _draw_graph(pnl_as_ratio, day_separated_time_series_df, pnl_trade_series, pnl_day_series)
    _to_csv(day_separated_time_series_df, time_series_df, stats_df, error_df, abnormal_positive, abnormal_negative)
    end = time.perf_counter()
    logger.info(f'process took {end - start} seconds')

def evaluate(ceiling_pair_df:pd.DataFrame):
    ceiling_pair_df.dropna(subset=['opened_at'], inplace=True)
    for index, row in ceiling_pair_df.iterrows():
        logger.info(index)
        logger.info(row['next_day_directory'])
        
        if is_checked:
            parts = row['next_day_directory'].split(sep=',')
            first_type = parts[1].strip()[1:-2]
            second_type = parts[0].strip()[2:-1]
            row['next_day_directory'] = [first_type, second_type]

        if not row['opened_at'] >= 0:
            logger.error(row['directory'])
            logger.warning(row['opened_at'])
            continue 
        if row['closed_in_first_day'] == True:

            logger.info(row['closed_at'])
            logger.info((ceiling_pair_df.loc[index, 'closed_at'] - ceiling_pair_df.loc[index, 'opened_at']) / ceiling_pair_df.loc[index, 'opened_at'])

            continue

        closing_price = evaluator(row['next_day_directory'], row['day_closed_at'], row['opened_at'], 0.03, 0.05).runner()
        if closing_price == False:
            continue
        ceiling_pair_df.loc[index, 'closed_at'] = closing_price

        logger.info((ceiling_pair_df.loc[index, 'closed_at'] - ceiling_pair_df.loc[index, 'opened_at']) / ceiling_pair_df.loc[index, 'opened_at'])
    ceiling_pair_df.to_csv(f'input/ceiling_directory_pairs_with_extra_columns_full_of_values_{int_start_date}_{int_end_date}.csv')

def check() -> pd.DataFrame:
    if is_spesific:
        ceiling_pair_df = pair_generator('input/EKSTRE_pairs_20240129_20240226.csv').generate_spesific_pairs()
    else:       
        ceiling_pair_df = pair_generator(PATH, int_start_date, int_end_date).generate_path_pairs()

    for index, pair in ceiling_pair_df.iterrows():

        pair['previous_close'] = getter(pair['previous_day_directory']).find_prev_day_closing_price()
        if pair['previous_close'] == 0:
            logger.error('in previous day there was no execution')
            continue

        result, entry_price, day_close_price, pos_enterence_index, pos_out_index = checker(pair['directory'], pair['previous_close']).check()
        ceiling_pair_df.loc[index, 'entering_index'] = pos_enterence_index 
        ceiling_pair_df.loc[index, 'closing_index'] = pos_out_index

        if result < 0:
            logger.warning(f"dropped security --> {ceiling_pair_df.loc[index, 'directory']}")
                        
            
            ceiling_pair_df.drop(index, inplace=True)
        elif result > 0:
            ceiling_pair_df.loc[index, 'opened_at'] = entry_price
            ceiling_pair_df.loc[index, 'closed_at'] = result
            ceiling_pair_df.loc[index, 'day_closed_at'] = day_close_price
            ceiling_pair_df.loc[index, 'closed_in_first_day'] = True 
        else:
            ceiling_pair_df.loc[index, 'opened_at'] = entry_price
            ceiling_pair_df.loc[index, 'day_closed_at'] = day_close_price

    ceiling_pair_df.to_csv(f'input/ceiling_directory_pairs_with_extra_columns_{int_start_date}_{int_end_date}.csv')
    return ceiling_pair_df

def _generate_series(filtered_df, day_separated_time_series_df):
    time_series_df = filtered_df.sort_values('date')
    pnl_trade_series = time_series_df['pnl_as_ratio'].cumsum()
    pnl_day_series = day_separated_time_series_df['day_pnl'].cumsum()
    pnl_as_ratio = filtered_df['pnl_as_ratio']*100
    return time_series_df, pnl_trade_series, pnl_day_series, pnl_as_ratio

def _calculate_statistic(filtered_df):
    mean_pnl = filtered_df['pnl_as_ratio'].mean() * 100
    median_pnl = filtered_df['pnl_as_ratio'].median() * 100
    total_pnl = filtered_df['pnl_as_ratio'].sum() * 100
    max_pnl = filtered_df['pnl_as_ratio'].max() * 100
    min_pnl = filtered_df['pnl_as_ratio'].min() * 100
    trade_count = filtered_df['pnl_as_ratio'].count()
    win_trade_count = filtered_df.loc[filtered_df["pnl_as_ratio"] > 0].shape[0] 
    loss_trade_count = filtered_df.loc[filtered_df["pnl_as_ratio"] < 0].shape[0]

    count_series = filtered_df.groupby('date').size()
    day_separated_time_series_df = filtered_df.groupby('date').agg('sum')
    day_separated_time_series_df['count'] = count_series
    day_separated_time_series_df['day_pnl'] = day_separated_time_series_df['pnl_as_ratio'] / day_separated_time_series_df['count']
    logger.info(day_separated_time_series_df)
    
    print(f'win day # {day_separated_time_series_df.loc[day_separated_time_series_df["pnl_as_ratio"] > 0].shape[0]}')
    print(f'loss day # {day_separated_time_series_df.loc[day_separated_time_series_df["pnl_as_ratio"] < 0].shape[0]}')

    daily_mean = day_separated_time_series_df['day_pnl'].mean()*100
    daily_median = day_separated_time_series_df['day_pnl'].median()*100
    daily_max = day_separated_time_series_df['day_pnl'].max()*100
    daily_min = day_separated_time_series_df['day_pnl'].min()*100
    daily_sum = day_separated_time_series_df['day_pnl'].sum()*100
    daily_win_count = day_separated_time_series_df.loc[day_separated_time_series_df["pnl_as_ratio"] > 0].shape[0]
    daily_loss_count = day_separated_time_series_df.loc[day_separated_time_series_df["pnl_as_ratio"] < 0].shape[0]

    day_separated_time_series_df['cum_prod'] = day_separated_time_series_df.day_pnl + 1
    day_separated_time_series_df['cum_prod'] = day_separated_time_series_df['cum_prod'].cumprod()

    stats_df = pd.DataFrame({
    'Statistic': [
        'Daily Mean', 'Daily Median', 'Daily Max', 'Daily Min', 'Daily Sum',
        'Mean PNL', 'Median PNL', 'Total PNL', 'Max PNL', 'Min PNL',
        'Trade Count', 'Win Trade Count', 'Loss Trade Count', 'Daily win count', 'Daily loss count'
    ],
    'Value (%)': [
        daily_mean, daily_median, daily_max, daily_min, daily_sum,
        mean_pnl, median_pnl, total_pnl, max_pnl, min_pnl,
        trade_count, win_trade_count, loss_trade_count, daily_win_count, daily_loss_count
    ]
})
    return day_separated_time_series_df, stats_df

def _to_csv(day_separated_time_series_df, time_series_df, stats_df, error_df, abnormal_positive, abnormal_negative):
    day_separated_time_series_df.to_csv(f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/daily_df.csv')
    time_series_df.to_csv(f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/per_trade_df.csv')
    stats_df.to_csv(f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/daily_statistics.csv', index=False)
    error_df.to_csv(f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/error_df.csv')
    if abnormal_negative.shape[0] > 0:
        abnormal_negative.to_csv(f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/abnormal_negative.csv')
    if abnormal_positive.shape[0] > 0:
        abnormal_positive.to_csv(f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/abnormal_positive.csv')


def _draw_graph(pnl_as_ratio, day_separated_time_series_df, pnl_trade_series, pnl_day_series):
    static_methods.draw_histogram(pnl_as_ratio, f'PnL frequency among all securities with %3 stop loss and %5 percent profit taking between {int_start_date} and {int_end_date}',
                                 'PnL as percentage', 'frequency',
                                f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/histogram_{int_start_date}_{int_end_date}.png')

    static_methods.draw_histogram(day_separated_time_series_df['day_pnl'], f'Daily PnL frequency among all securities with %3 stop loss and %5 percent profit taking between {int_start_date} and {int_end_date}',
                                'PnL as percentage', 'frequency',
                                f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/histogram_day_{int_start_date}_{int_end_date}.png')

    static_methods.draw_scatter(range(len(pnl_trade_series)), pnl_trade_series, f'rolling cumulative pnl per trade between {int_start_date} and {int_end_date}',
                                'trade count', 'pnl',
                                f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/line_graph_trade_{int_start_date}_{int_end_date}.png')

    data_x = pd.to_datetime([str(int(date)) for date in pnl_day_series.index.to_list()], format='%Y%m%d')

    static_methods.draw_scatter(data_x, pnl_day_series.values, f'rolling cumulative pnl per day between {int_start_date} and {int_end_date}',
                                'date', 'pnl',
                                f'/home/gkatayifci/analysis/tob_ceiling_analysis/output/{OUTPUT_DIR}/line_graph_per_day_{int_start_date}_{int_end_date}.png')
    


if __name__ == "__main__":
    main()
