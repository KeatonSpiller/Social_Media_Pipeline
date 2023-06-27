# %%
# import libraries
import pandas as pd, os, sys, statsmodels.api as sm
from datetime import date 
import datetime

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
# HISTORICAL DATA 

# **** BY DAY ****
# stocks
historical_stocks_byday_df = pd.read_parquet('./data/transformed/stocks/stock_tickers_byday_norm.parquet',
                            engine= 'pyarrow',
                            dtype_backend = 'pyarrow')
historical_stocks_byday_df['date'] = pd.to_datetime(historical_stocks_byday_df['date']).dt.date
# twitter
historical_twitter_byday_df = pd.read_parquet('./data/transformed/twitter/pivot_user_wkd_merge_byday.parquet', 
                             engine= 'pyarrow',
                             dtype_backend = 'pyarrow')
historical_twitter_byday_df['date'] = pd.to_datetime(historical_twitter_byday_df['date']).dt.date
# merge
historical_merge_byday_df = pd.merge(historical_stocks_byday_df, historical_twitter_byday_df, how='inner', on='date').fillna(0)
# export
df_to_parquet(df = historical_merge_byday_df, 
        folder = f'./data/transformed/merged', 
        file = f'/historical_merge_byday.parquet')
# **** BY HOUR ****
index = 'timestamp'
# stocks
historical_stocks_byhour_df = pd.read_parquet('./data/transformed/stocks/stock_tickers_byhour_norm.parquet',
                            engine= 'pyarrow',
                            dtype_backend = 'pyarrow')
historical_stocks_byhour_df = historical_stocks_byhour_df.set_index(index).astype('float64[pyarrow]').reset_index()
# twitter
historical_twitter_byhour_df = pd.read_parquet('./data/transformed/twitter/pivot_user_wkd_merge_byhour_wrkhrs.parquet', 
                             engine= 'pyarrow',
                             dtype_backend = 'pyarrow')
historical_twitter_byhour_df = historical_twitter_byhour_df.set_index(index).astype('float64[pyarrow]').reset_index()
# merge
historical_merge_byhour_df = pd.merge(historical_twitter_byhour_df, historical_stocks_byhour_df, how='inner', on=index).fillna(0)
# export
df_to_parquet(df = historical_merge_byhour_df, 
        folder = f'./data/transformed/merged', 
        file = f'/historical_merge_byhour.parquet')

# %% 
# TEST DATA

# TODAY
today = date.today()
# **** BY MINUTE ****
twitter_by_minute_df = pd.read_parquet('./data/transformed/twitter/pivot_user_wkd_merge_by_minute_wrkhrs.parquet', 
                                                engine= 'pyarrow',
                                                dtype_backend = 'pyarrow')
todays_twitter_minute_df = twitter_by_minute_df[pd.to_datetime(twitter_by_minute_df[index]).dt.date == today]
todays_stocks_minute_df = pd.read_parquet('./data/transformed/stocks/stock_tickers_norm_by_minute_today.parquet', 
                                    engine= 'pyarrow',
                                    dtype_backend = 'pyarrow')
# Consider forward filling instead of many twitter zeros
# df = df.set_index('timestamp').groupby('id', sort=False)['data'].resample('1min').ffill()
todays_merge_df = pd.merge(todays_stocks_minute_df, todays_twitter_minute_df, how='inner', on=index)
df_to_parquet(df = todays_merge_df, folder = f'./data/transformed/merged', file = f'/todays_twitter_stock_merge_byminute.parquet')
# **** BY HOUR ****
todays_twitter_hour_df = historical_twitter_byhour_df[pd.to_datetime(historical_twitter_byhour_df[index]).dt.date == today].reset_index(drop=True)

todays_stocks_byhour_df = pd.read_parquet('./data/transformed/stocks/stock_tickers_norm_by_hour_today.parquet', 
                                    engine= 'pyarrow',
                                    dtype_backend = 'pyarrow').reset_index(drop=True)
todays_merge_hour_df = pd.merge(todays_twitter_hour_df,todays_stocks_byhour_df, how='inner', on=index)
df_to_parquet(df = todays_merge_hour_df, folder = f'./data/transformed/merged', file = f'/todays_twitter_stock_merge_byhour.parquet')
todays_merge_hour_df

# ********************************************************************
# Test volatility of Market
# predict price of stock and between -1 to 1 how good
# predict for today | 1 week | 2 weeks ahead by HOUR
# Build a pipeline of models simple to complex

# ********************************************************************

# 1 WEEK ( Simulated )
# **** BY HOUR ****

# 2 WEEK ( Simulated )
# **** BY HOUR ****


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
