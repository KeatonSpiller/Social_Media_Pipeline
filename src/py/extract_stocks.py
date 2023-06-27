# %% 
# Import Libraries
import os,pandas as pd, sys, yfinance as yf
from datetime import date, timedelta
import pytz

# %%            
# - Change Directory to top level folder
top_level_folder = 'Social_Media_Pipeline'
if(os.getcwd().split(os.sep)[-1] != top_level_folder):
    infinite_limit, infinity_check = 10, 0
    try:
        while(os.getcwd().split(os.sep)[-1] != top_level_folder):
            os.chdir('..') # move up a directory
            infinity_check += 1
            if(infinity_check > infinite_limit):
                raise Exception("cwd: {os.getcwd()}: {infinite_limit} directories up from {top_level_folder}")
        print(f"cwd: {os.getcwd()}", sep = '\n')
        # add path to system path for running in terminal
        if(os.getcwd() not in sys.path):
            sys.path.append(os.getcwd())
    except Exception as e:
        print(f"cwd: {os.getcwd()}", sep = '\n')
        print(f"{e}\n:Please start current working directory from {top_level_folder}")

# import local files
from extract_stocks_tools import *

# %% [markdown]
# - Read in Stocks to download from yahoo finance
with open(os.path.normpath(os.getcwd() + './user_input/stock_tickers.xlsx'), 'rb') as f:
    ticker_df = pd.read_excel(f, sheet_name='ticker_sheet')
    ticker_df = ticker_df.where(pd.notnull(ticker_df), '')
    f.close()
stock_str = " ".join(ticker_df.ticker_name)

# %%
# Cross reference Twitter for how far back to download
file = './data/transformed/twitter/pivot_user_wkd_merge_byday.parquet'
today = date.today()
if os.path.exists(file):
    df = pd.read_parquet(file, engine= 'pyarrow', dtype_backend = 'pyarrow')
    how_far_back = df.date.min().date()
else:
    how_far_back = '2000-01-01'
print(f'{how_far_back} -> {today}')

# Columns to rename
column_rename_date  =(({'Date':'date'}) | dict(zip(ticker_df.ticker_name, ticker_df.ticker_label)))
column_rename_datetime  =(({'Datetime':'timestamp'}) | dict(zip(ticker_df.ticker_name, ticker_df.ticker_label)))
columns_to_normalize = list(ticker_df.ticker_label)

# %%
# ***** HISTORICAL By DAY *****

# Download Range of Stocks by day
historical_stocks_byday_df = download_historical_stocks(stocks_to_download=stock_str,
                                                        columns_to_rename = column_rename_date,
                                                        how_far_back=how_far_back,
                                                        upto=today,
                                                        index='date',
                                                        file='stock_tickers_byday',
                                                        folder=f'./data/extracted/merged/stocks',
                                                        period='1d',
                                                        interval='1d')
# Min Max Normalize historical Stocks by day
normalize_historical_stocks(df=historical_stocks_byday_df.copy(),
                            columns= columns_to_normalize, 
                            file="stock_tickers_byday_norm",
                            folder=f'./data/transformed/stocks')

# %%
# ***** HISTORICAL BY HOUR *****
# Download Range of Stocks by hour
historical_stocks_byhour_df = download_historical_stocks(stocks_to_download=stock_str,
                                                        columns_to_rename = column_rename_datetime,
                                                        how_far_back=how_far_back,
                                                        upto=today,
                                                        index = 'timestamp',
                                                        file='stock_tickers_byhour',
                                                        folder=f'./data/extracted/merged/stocks',
                                                        period='1d',
                                                        interval='60m')
# Min Max Normalize historical Stocks by hour ( smallest available interval to download )
normalize_historical_stocks(df=historical_stocks_byhour_df.copy(),
                            columns= columns_to_normalize, 
                            file="stock_tickers_byhour_norm",
                            folder=f'./data/transformed/stocks')
# %%
# ***** TODAY BY MINUTE *****
todays_stocks_byminute_df = download_todays_stocks(stocks_to_download=stock_str, 
                                        columns_to_rename=column_rename_datetime,
                                        index='timestamp',
                                        file="stock_tickers_minute_today",
                                        folder=f'./data/extracted/merged/stocks',
                                        period='1d',
                                        interval='1m')
# Min Max Normalize Todays Stocks 
normalize_todays_stocks(df_today=todays_stocks_byminute_df,
                        df_historical=historical_stocks_byday_df.copy(), 
                        columns= columns_to_normalize, 
                        file="stock_tickers_norm_by_minute_today",
                        folder=f'./data/transformed/stocks')
# %%
# ***** TODAY BY HOUR *****
yesterday = date.today() + timedelta(days=-1)
tommorow = date.today() + timedelta(days=1)
todays_stocks_byhour_df = download_historical_stocks(
                                        stocks_to_download=stock_str, 
                                        columns_to_rename=column_rename_datetime,
                                        how_far_back=today,
                                        upto=tommorow, # exclusive
                                        index='timestamp',
                                        file="stock_tickers_hour_today",
                                        folder=f'./data/extracted/merged/stocks',
                                        period='1d',
                                        interval='60m')
# Min Max Normalize Todays Stocks 
normalize_todays_stocks(df_today=todays_stocks_byhour_df,
                        df_historical=historical_stocks_byday_df.copy(), 
                        columns= columns_to_normalize, 
                        file="stock_tickers_norm_by_hour_today",
                        folder=f'./data/transformed/stocks')
todays_stocks_byhour_df
# %%