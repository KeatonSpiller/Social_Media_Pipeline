# %%
# import libraries
import pandas as pd, os, sys, statsmodels.api as sm
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import normalize
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MaxAbsScaler
from sklearn.preprocessing import RobustScaler
from datetime import date, timedelta
from sklearn.feature_selection import SequentialFeatureSelector
from sklearn.neighbors import KNeighborsClassifier

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
# Stocks MinMax normalized
historical_stocks_byhour_df = pd.read_parquet('./data/transformed/stocks/stock_tickers_byhour_norm.parquet',
                            engine= 'pyarrow',
                            dtype_backend = 'pyarrow')
historical_stocks_byhour_df = historical_stocks_byhour_df.set_index(index).astype('float64[pyarrow]').reset_index()
# Original closing prices
stocks_byhour_non_norm = pd.read_parquet('./data/extracted/merged/stocks/stock_tickers_byhour.parquet',
                            engine= 'pyarrow',
                            dtype_backend = 'pyarrow')
stocks_byhour_non_norm = stocks_byhour_non_norm.set_index(index).astype('float64')

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
dayofweek = today.weekday() # Range (0 : mon -> 6 : Sun)

# IF Weekend Set today to Last friday to align with stock market
if(dayofweek  == 5):
    today = today.replace(day = today.day - 1)
if(dayofweek == 6):
    today = today.replace(day = today.day - 2)
# %%
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
# Original closing prices
todays_stocks_byhour_non_norm = pd.read_parquet('./data/extracted/merged/stocks/stock_tickers_hour_today.parquet',
                            engine= 'pyarrow',
                            dtype_backend = 'pyarrow')
todays_stocks_byhour_non_norm = todays_stocks_byhour_non_norm.set_index(index).astype('float64')

todays_merge_hour_df = pd.merge(todays_twitter_hour_df,todays_stocks_byhour_df, how='inner', on=index)
df_to_parquet(df = todays_merge_hour_df, folder = f'./data/transformed/merged', file = f'/todays_twitter_stock_merge_byhour.parquet')
todays_merge_hour_df

# ********************************************************************
# Test volatility of Market
# predict price of stock and between -1 to 1 how good
# predict for today | 1 week | 2 weeks ahead by HOUR
# Build a pipeline of models simple to complex
# moving averages ? (SMA, EMA, MACD), the Relative Strength Index (RSI), Bollinger Bands (BBANDS)

# ********************************************************************

# 1 WEEK ( Simulated )
# **** BY HOUR ****

# 2 WEEK ( Simulated )
# **** BY HOUR ****

# %%

# LINEAR MODEL
# **** TRAIN ****
# non time series
label = 'APPLE'
train_df = historical_merge_byhour_df.copy().set_index(index).astype('float32')

# robust scaling ->
# (df[c] - df[c].median()) / (df[c].quantile(0.75) - df[c].quantile(0.25))
scaler = RobustScaler()
scaler.fit(train_df)
train_df = pd.DataFrame(scaler.transform(train_df), index= train_df.index, columns = train_df.columns).astype('float32')

# ************** Y ******************
# normalized
# y = train_df.loc[:,label]
# non normalized
y = stocks_byhour_non_norm.loc[stocks_byhour_non_norm.index.isin(train_df.index),label].astype('float32')

# *********** X ***************
# stocks and twitter
# drop y label
X = train_df.drop( columns = [label] )
# drop today from training to avoid over training
X = X.loc[pd.to_datetime(train_df.index).date != today,:]

# twitter w/out stocks
# twitter_X = historical_twitter_byhour_df.set_index(index).copy()
# X = twitter_X.loc[twitter_X.index.isin(train_df.index),:].drop(columns = ['favorite_count', 'retweet_count'] ).astype('float32')
# X = train_df.loc[:,'bigram_CNN':'unigram_guardian']
# X = twitter_X.loc[train_df.index.isin(twitter_X.index),:]

# For Cross Validation to minimize bias/varaince
# X_train, X_train_eval, y_train, y_train_eval = train_test_split(X, y, test_size=0.8, random_state=42)

# statsmodels
sm_lm = sm.OLS(endog=y, exog=sm.add_constant(X)).fit() # X = exog Y  = endog
# sklearn
lm= LinearRegression(fit_intercept = True, copy_X = True).fit(X=X, y=y)
# print(sm_lm.summary())

# # sequential feature selector
# sfs = SequentialFeatureSelector(lm, n_features_to_select=3)
# fit = sfs.fit(X, y)
# trf = sfs.transform(X)
# trf_column = list(sfs.get_feature_names_out())
# trf_X = pd.DataFrame(trf, columns = trf_column, index=X.index)
# lm= LinearRegression(fit_intercept = True, copy_X = True).fit(X=trf_X, y=y)

# predict
train_predictions = lm.predict(X)
# # scores = cross_val_score(lm, X, y, cv=5)

# Visualize True vs predicted Values
fig, ax = plt.subplots()
# plt.scatter(x = y_train_eval, 
#             y = train_predictions)

plt.plot(y.index, y, "ro", label="True Values")
plt.plot(X.index, train_predictions, "bo", label="Predictions", )
plt.xticks(rotation=90)
plt.xlabel("Day")
plt.ylabel(f"Price of {label}")
ax.legend()
# %%
# ********** OUTLIERS ??? ************
train_df[(train_df > 2).any(axis=1)]
# train_df[train_df.isin([np.nan, np.inf, -np.inf]).any(axis=1)]
 # %%
 
# # **** TEST ****
todays_merge_hour_df_norm = todays_merge_hour_df.copy().set_index(index).astype('float32')
# # to account for the many zero's forward fill
# todays_merge_hour_df_norm = todays_merge_hour_df_norm.ffill()
test_df = todays_merge_hour_df_norm.copy()

# ************** Y ******************
# normalized y (% and porportions)
# y_test = test_df.loc[:,label]
# non normalized y
y_test = todays_stocks_byhour_non_norm.loc[todays_stocks_byhour_non_norm.index.isin(test_df.index),label].astype('float32')
# *********** X ***************
# stocks and twitter
X_test = test_df.loc[:,~(test_df.columns==label)]
# using X values removed from p value significance
# X_test = X_test.loc[:,trf_X.columns]
# twitter w/out stocks
# # For Cross Validation to minimize bias/varaince
# X_test, X_test_eval, y_test, y_test_eval = train_test_split(X_test, y_test, test_size=0.8, random_state=42)

# statsmodels
sm_lm = sm.OLS(endog=y_test, exog=sm.add_constant(X_test)).fit() # X = exog Y  = endog
# sklearn
test_predictions = lm.predict(X_test)
# print(sm_lm.summary())

fig, ax = plt.subplots()
# ax.scatter(x = y_test, y = test_predictions)
time_axis = pd.to_datetime(y_test.index).strftime('%I:%M\n %d/%m\n %Y\n')

print(time_axis)

plt.plot(time_a, "-r", label="True Values")
plt.plot(time_axis, test_predictions, "-b", label="Predictions", )
# plt.xticks(rotation=90)

plt.xlabel("Hours 9:30AM to 3:30PM EST")
plt.ylabel(f"Price of {label}")
ax.legend()

# %%
# Build Target and predict
# Xnew = sm.add_constant(todays_merge_df.set_index('date'), has_constant='add')
# model = {} # Model Build For Each index fund
# # output = pd.DataFrame(columns=['index', 'prediction'])
# stock_list = list(historical_stocks_df.set_index('date').columns)
# for t in stock_list:
#     data_with_target = create_target(historical_merge_df.copy(), day = 5, ticker = t).set_index('date')
#     m = linear_model(data_with_target,split=0.20,summary = False)
#     y_pred = m['lm'].predict(Xnew)
#     model[t] = (y_pred, m)
# print(model)
# %%
