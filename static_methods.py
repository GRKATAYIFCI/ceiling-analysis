import pandas as pd
from loguru import logger
import pandas.errors
import matplotlib.pyplot as plt
import re

PRICE_TICK_SIZE_MAP: dict[float, float] = {
    20.00: 0.01,
    50.00: 0.02,
    100.00: 0.05,
    250.00: 0.10,
    500.00: 0.25,
    1000.00: 0.50,
    2500.00: 1.0,
    99999999999.00: 2.50
}

def shrink_df_to_only_tradeable_times(df):

    data = [[1707202509213163371, 'FORTE', 'A', 'S', 1, 688, 85.0, 85.1, 150, 1, 7821059038554431808]]
    columns = ["time", "security_name", "recent_message_type", "order_type", "1", "bid_qty", "bid_price", "ask_price", "ask_qty", "2", "ID"]
    surekli_islem_messages = df.loc[df.order_type.str.contains('P_SUREKLI_ISLEM')].index

    o_messages = df.loc[df['recent_message_type'] == 'O'].index
    #logger.debug(f"length of surekli islem message list {len(surekli_islem_messages)}")
    if len(surekli_islem_messages) == 0:
        logger.critical(f"{df['security_name']} is not continous stock")
        df = pd.DataFrame(data, columns=columns)
        return df
    
    pattern = re.compile(r'\.R$')
    try:
        pattern_df = df.loc[df['security_name'].str.endswith('.R')]
        pattern_index = pattern_df.index[-1]
        df = df.loc[pattern_index + 1:]
        logger.critical(f"pattern found {pattern_index}")

    except KeyError as e:
        logger.debug(e)
    except IndexError as e:
        logger.debug(e)

    df = _get_df(df, surekli_islem_messages, o_messages)
    return df


def _get_df(df, surekli_index, o_messages_index):
    indices = []
    for i in range(len(o_messages_index) - 1):
        if o_messages_index[i] in surekli_index:
            indices.append([o_messages_index[i], o_messages_index[i + 1]])

    slices = []
    for k in indices:
        logger.info(f"{k[0]} {k[1]} {df['security_name'].iloc[-1]}")
        temporary_df = df.loc[k[0]:k[1], :]
        slices.append(temporary_df)
    
    new_df = pd.concat(slices, ignore_index=False)
    return new_df

def calculate_tick_size(input_price):

    for price, tick_size in PRICE_TICK_SIZE_MAP.items():
        if input_price <= price:
            return tick_size

def _convert(df):
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(df[col])
        except:
            df[col] = df[col]
    return df

def read_csv(PATH):

    data = [[1707202509213163371, 'FORTE', 'A', 'S', 1, 688, 85.0, 85.1, 150, 1, 7821059038554431808]]
    columns = ["time", "security_name", "recent_message_type", "order_type", "1", "bid_qty", "bid_price", "ask_price", "ask_qty", "2", "ID"]
    columns_v2 = ["time", "security_name", "recent_message_type", "1", "bid_qty", "bid_price", "ask_price", "ask_qty", "2", "ID"]
    try:
        df =  pd.read_csv(PATH[0], header=None)
        df = _convert(df)

    except FileNotFoundError:
        try:
            df =  pd.read_csv(PATH[1], header=None)
            df = _convert(df)
        except FileNotFoundError:
            df = pd.DataFrame(data, columns=columns)
            logger.error(f"{PATH} cannot found")
            return df

        except pandas.errors.EmptyDataError:
            df = pd.DataFrame(data, columns=columns)
            logger.error(f'{PATH} is empty')
            return df
    except pandas.errors.EmptyDataError:
            df = pd.DataFrame(data, columns=columns)
            logger.error(f'{PATH} is empty')
            return df
    try:
        df.columns = columns
    except ValueError:
        print(df)
        df.columns = columns_v2
    

    return df



def draw_histogram(data, title, x, y='frequency', PATH='output/histogram.png'):

    # Plotting the distribution
    plt.figure(figsize=(12, 8))
    plt.hist(data, bins=25, edgecolor='black')
    plt.title(title)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.grid(True)
    plt.savefig(PATH)

def draw_scatter(data_x, data_y, title, x, y, PATH):   

    plt.figure(figsize=(12, 8))
    plt.plot(data_x, data_y, color='blue',linestyle='-', alpha=0.5)

    plt.title(title)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.grid(True)
    plt.savefig(PATH)
