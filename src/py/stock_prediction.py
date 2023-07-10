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
# from mc_simulation import AverageModel as am
# from mc_simulation import VolatilityModel as vm
# from mc_simulation import MonteCarloSimulation as mcs

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

# %%

# **** LINEAR MODEL TRAIN BY [HOUR] ****
# non time series
label = 'APPLE'
train_df = historical_merge_byhour_df.copy().set_index(index).astype('float32')
# ************** scaling ******************
# robust scaling -> removing median and upper/lower percentiles
# (df[c] - df[c].median()) / (df[c].quantile(0.75) - df[c].quantile(0.25))
scaler = RobustScaler()
scaler.fit(train_df)
train_df = pd.DataFrame(scaler.transform(train_df), index= train_df.index, columns = train_df.columns).astype('float32')
# ************** Y train ******************
# normalized
# y = train_df.loc[:,label]
# non normalized
y = stocks_byhour_non_norm.loc[stocks_byhour_non_norm.index.isin(train_df.index),label].astype('float32')
# *********** X train ***************
# twitter and stocks
# drop y label
X = train_df.drop( columns = [label] )
# drop today from training to avoid over training
X = X.loc[pd.to_datetime(train_df.index).date != today,:]
# twitter without stocks
twitter_prob_column = X.columns[X.columns.str.contains(pat="bigram_|unigram_|trigram_")]
X_twitter_only = X.loc[:,twitter_prob_column]
#**** BUILD Linear Model training dataset for X and y ****
# statsmodels lm
sm_lm = sm.OLS(endog=y, exog=sm.add_constant(X)).fit() # X = exog Y  = endog
# Statistics Summary
# print(sm_lm.summary()) 
# sklearn
lm= LinearRegression(fit_intercept = True, copy_X = True).fit(X=X, y=y)
### *** Cross Validation *** ####
# Cross Validation to minimize bias/varaince with sampling split for better trained model
# X_train, X_train_eval, y_train, y_train_eval = train_test_split(X, y, test_size=0.8, random_state=42)
# scores = cross_val_score(lm, X, y, cv=5)
#**** # remove high p value variables ****
# sfs = SequentialFeatureSelector(lm, n_features_to_select=5)
# fit = sfs.fit(X, y)
# trf = sfs.transform(X)
# trf_X = pd.DataFrame(trf, columns = list(sfs.get_feature_names_out()), index=X.index)
# lm= LinearRegression(fit_intercept = True, copy_X = True).fit(X=trf_X, y=y)
# **** Visualize Linear Model of TRAINING DATASET *****
test_split = 0.25
train_split = 1 - test_split
X_train, X_train_eval, y_train, y_train_eval = train_test_split(X, y, test_size=test_split, random_state=42)
lm_test= LinearRegression(fit_intercept = True, copy_X = True).fit(X=X_train, y=y_train)
train_predictions = lm_test.predict(X_train_eval)
fig, ax = plt.subplots()
time_axis_train = pd.to_datetime(y_train_eval.index)
plt.plot(time_axis_train, y_train_eval, "ro", label="True Values Training Data")
plt.plot(time_axis_train, train_predictions, "bo", label="Predictions Training Data", )
ax.set_title(label=f'Predictions From Training Data (split -> train: {int(train_split * 100)}% test: {int(test_split* 100)}%)')
plt.xticks(rotation=90)
plt.xlabel("Day")
plt.ylabel(f"Price of {label}")
ax.legend()
# ********** OUTLIERS ??? ************
# train_df[(train_df > 2).any(axis=1)]
# train_df[train_df.isin([np.nan, np.inf, -np.inf]).any(axis=1)]
# %%

# **** LINEAR MODEL TEST BY [HOUR]  ****
# ************* TODAY FOR PRICE NOW *******************
test_df = todays_merge_hour_df.copy().set_index(index).astype('float32')
# ************** scaling ******************
# robust scaling -> removing median and upper/lower percentiles
# (df[c] - df[c].median()) / (df[c].quantile(0.75) - df[c].quantile(0.25))
# scaler = RobustScaler()
# scaler.fit(test_df)
# test_df = pd.DataFrame(scaler.transform(test_df), index= test_df.index, columns = test_df.columns).astype('float32')
# ************** Y TEST ******************
# normalized y
# y_test = test_df.loc[:,label]
# non normalized y
y_test = todays_stocks_byhour_non_norm.loc[todays_stocks_byhour_non_norm.index.isin(test_df.index),label].astype('float32')
# *********** X TEST ***************
# stocks and twitter
X_test = test_df.loc[:,~(test_df.columns==label)]
#**** BUILD Linear Model test dataset****
# statsmodel
sm_lm_test = sm.OLS(endog=y_test, exog=sm.add_constant(X_test)).fit() # X = exog Y  = endog
print(sm_lm_test.summary())
# sklearn
test_predictions = lm.predict(X_test)
# plot
fig, ax = plt.subplots()
time_axis = pd.to_datetime(y_test.index).strftime('%I:%M\n %m/%d\n %Y\n')
plt.plot(time_axis, y_test, "-r", label="True Values")
plt.plot(time_axis, test_predictions, "-b", label="Predictions", )
plt.xlabel("Hours 9:30AM to 3:30PM EST")
plt.ylabel(f"Price of {label}")
ax.legend()

# %%
# 1 WEEK ( Simulated ) to next Friday
# **** LINEAR MODEL TEST BY [HOUR]  ****
# **** (1 Week Simulated) ****
sample_from = X_test.copy()

# Generate Next Week's timestamps by hour,workday,workweek in (EST Military)
start_range = pd.to_datetime(sample_from.index.max())
start_range_normalized = start_range.normalize()
weekday = start_range_normalized.weekday() # Monday is 0 and Sunday is 6
end_range_normalized = start_range_normalized.replace(day = int(start_range_normalized.day) + 8, hour=23, minute=59)
start_workday = "09:30"
end_workday = "15:30"
week_range_all = pd.bdate_range(start=start_range_normalized, end=end_range_normalized, freq='H',tz='America/New_York')
week_range_all = week_range_all.drop(week_range_all.max())
week_range_offset = week_range_all + pd.tseries.offsets.DateOffset(minutes = 30)
week_range_mask = week_range_offset.indexer_between_time(start_workday, end_workday)
week_range = week_range_offset[week_range_mask]

historical_mean = X.mean()
historical_std = X.std()

# Simulate Samples from the last prices against randomized variations of the std and mean
sample_count = len(sample_from)
choice = [1,-1]
simulated_results = pd.DataFrame()
# for h in week_range[0:10]:
#     random_sample_from_today = sample_from.iloc[np.random.randint(sample_count)]
#     random_norm = np.random.normal()
#     sub_or_add = np.random.choice(choice)
#     sample = random_sample_from_today + (sub_or_add * (random_norm + (historical_std)))
#     pd.DataFrame([sample[:]], columns = sample.index, index= h)
    # sub_df = pd.DataFrame(sample, columns=sample_from.columns)
    # sub_df['timestamp'] = h
    # sub_df.set_index(index)
    # print(sub_df)
    # simulated_results = pd.concat([simulated_results, sub_df])
    # print(simulated_results)



# 2 WEEK ( Simulated )
# **** BY HOUR ****

# %% Previous Implementation 
# FOR EVERY STOCK prediction BETWEEN (-1 BAD to 1 GOOD)
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
