# %%
# import libraries
import pandas as pd, os, sys, statsmodels.api as sm
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
from stock_prediction_tools import *

# %% 
# Historical stocks by day
historical_stocks_byday_df = pd.read_parquet('./data/transformed/stocks/stock_tickers_byday_norm.parquet',
                            engine= 'pyarrow',
                            dtype_backend = 'pyarrow')
# Historical stocks by hour
historical_stocks_byhour_df = pd.read_parquet('./data/transformed/stocks/stock_tickers_byhour_norm.parquet',
                            engine= 'pyarrow',
                            dtype_backend = 'pyarrow')
# %%
# Historical twitter by day
historical_twitter_byday_df = pd.read_parquet('./data/transformed/twitter/pivot_user_wkd_merge_byday.parquet', 
                             engine= 'pyarrow',
                             dtype_backend = 'pyarrow')
# Historical twitter by hour
historical_twitter_byhour_df = pd.read_parquet('./data/transformed/twitter/pivot_user_wkd_merge_byhour.parquet', 
                             engine= 'pyarrow',
                             dtype_backend = 'pyarrow')

# Merging historical twitter probabilities and ticker prices
# by day
historical_merge_byday_df = pd.merge(historical_stocks_byday_df, historical_twitter_byday_df, how='inner', on='date').fillna(0)
# Export historical twitter and stock merge
df_to_parquet(df = historical_merge_byday_df, 
        folder = f'./data/transformed/merged', 
        file = f'/historical_merge_byday.parquet')

# Merging historical twitter probabilities and ticker prices
# by hour
historical_merge_byhour_df = pd.merge(historical_stocks_byhour_df, historical_twitter_byhour_df, how='inner', on='date').fillna(0)
# Export historical twitter and stock merge
df_to_parquet(df = historical_merge_byhour_df, 
        folder = f'./data/transformed/merged', 
        file = f'/historical_merge_byhour.parquet')

# %% Todays Data
today = date.today()
todays_twitter_df = historical_twitter_byhour_df[historical_twitter_byhour_df['date'] == today]
todays_stocks_df = pd.read_parquet('./data/transformed/stocks/todays_stock_tickers_norm.parquet', 
                                    engine= 'pyarrow',
                                    dtype_backend = 'pyarrow')
# %%
# ***************************************************************************************************************
# historical_stocks_byhour_df

# %%
todays_merge_df = pd.merge(todays_stocks_df, todays_twitter_df, how='inner', on='date')
df_to_parquet(df = todays_merge_df, folder = f'./data/transformed/merged', file = f'/todays_twitter_stock_merge.parquet')
# %%
# Build Target and predict
Xnew = sm.add_constant(todays_merge_df.set_index('date'), has_constant='add')
model = {} # Model Build For Each index fund
# output = pd.DataFrame(columns=['index', 'prediction'])
stock_list = list(historical_stocks_df.set_index('date').columns)
for t in stock_list:
    data_with_target = create_target(historical_merge_df.copy(), day = 5, ticker = t).set_index('date')
    m = linear_model(data_with_target,split=0.20,summary = False)
    y_pred = m['lm'].predict(Xnew)
    model[t] = (y_pred, m)
print(model)
# %%

# predicting by minute
# historical_stocks_df_byminute = pd.read_parquet('./data/transformed/stocks/stock_tickers_norm.parquet',
#                             engine= 'pyarrow',
#                             dtype_backend = 'pyarrow')
# historical_twitter_df_byminute = pd.read_parquet('./data/transformed/twitter/cleaned_twitter_ngram_norm.parquet', 
#                              engine= 'pyarrow',
#                              dtype_backend = 'pyarrow')

# historical_twitter_by_minute = historical_twitter_df_byminute.loc[:, ['created_at', 'favorite_count', 'retweet_count', 'unigram_probability', 'bigram_probability']]
# historical_twitter_by_minute

# %%
