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
# - Read in Stocks to download from yahoo finance
with open(os.path.normpath(os.getcwd() + './user_input/stock_tickers.xlsx'), 'rb') as f:
    ticker_df = pd.read_excel(f, sheet_name='ticker_sheet')
    ticker_df = ticker_df.where(pd.notnull(ticker_df), '')
    f.close()
stock_str = " ".join(ticker_df.ticker_name)

# %% [markdown]
# - Determine date range to download stocks
# (i.e., start='2000-01-01' to end='2023-01-01')
file = './data/transformed/pivot_user_by_date_wkd_merge.parquet'
today = date.today()
if os.path.exists(file):
    df = pd.read_parquet(file, engine= 'pyarrow', dtype_backend = 'pyarrow')
    how_far_back = df.date.min().date()
else:
    how_far_back = '2000-01-01'
n = len(ticker_df.ticker_label) # how many tickers
print(f'{how_far_back} -> {today}')

# Columns to rename
column_rename  =(({'Date':'date'}) | dict(zip(ticker_df.ticker_name, ticker_df.ticker_label)))

# %%
# Download closing prices for Stocks from date Range
stock_tickers_df = yf.download(stock_str,
                               how_far_back,
                               today,
                               interval = '1d',
                               progress=False)['Close'].reset_index().rename(columns=column_rename).fillna(0)
# %%
# convert to pyarrow
stock_tickers_df = stock_tickers_df.set_index('date')
stock_tickers_df = stock_tickers_df.astype('float64[pyarrow]').reset_index().astype({'date':'datetime64[ns]'})
stock_tickers_df.head()
stock_tickers_df.dtypes
# %%
# export original
df_to_parquet(df = stock_tickers_df, 
          folder = f'./data/extracted/merged', 
          file = f'/stock_tickers.parquet')
# Min Max normalize tickers
columns = list(ticker_df.ticker_label)
stock_tickers_df_norm = normalize_columns(stock_tickers_df.copy(), columns)
stock_tickers_df_norm.head()
# %%
# export normalized
df_to_parquet(df = stock_tickers_df_norm, 
          folder = f'./data/extracted/merged', 
          file = f'/stock_tickers_norm.parquet')
# %%