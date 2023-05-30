# - Import Libraries
import os,pandas as pd, sys, yfinance as yf
from datetime import date

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
from extract_tools import df_to_parquet, normalize_columns

# %% [markdown]
# - Read in Ticker labels
with open(os.path.normpath(os.getcwd() + './user_input/stock_tickers.xlsx'), 'rb') as f:
    ticker_df = pd.read_excel(f, sheet_name='ticker_sheet')
    ticker_df = ticker_df.where(pd.notnull(ticker_df), '')
    f.close()

# %% [markdown]
# - Downloding stock tickers values
# how_far_back = df_wide.index.min().date()
file = './data/transformed/pivot_user_by_date_wkd_merge.parquet'
if os.path.exists(file):
    df = pd.read_parquet(file)
    how_far_back = df.date.min().date()
else:
    how_far_back = '1980-01-01'
today = date.today()
print(f'{how_far_back} -> {today}')
column_names = dict(zip(ticker_df.ticker_name, ticker_df.ticker_label))
column_names['Date']='date'
stock_list = list(ticker_df.ticker_name)
stock_str = ' '.join( stock_list )
# downloading stock tickers from list
stock_tickers_df = yf.download(stock_str, how_far_back, today, interval = '1d', progress=False)['Close'].reset_index('Date').rename(columns=column_names).fillna(0)

# converting to float and aligning date to the same data type
convert_dict = dict(zip(ticker_df.ticker_label, ['float64']*len(ticker_df.ticker_label)))
convert_dict['date'] = 'datetime64[ns]'
stock_tickers_df = stock_tickers_df.astype(convert_dict)
# export original
df_to_parquet(df = stock_tickers_df, 
          folder = f'./data/extracted/merged', 
          file = f'/stock_tickers.parquet')

# Min Max normalize ticker columns and favorite/retweet counts
columns = list(ticker_df.ticker_label) + ['favorite_count', 'retweet_count']
df_merge = normalize_columns(stock_tickers_df.copy(), columns)
# export normalized
df_to_parquet(df = stock_tickers_df, 
          folder = f'./data/extracted/merged', 
          file = f'/stock_tickers_norm.parquet')
# %%