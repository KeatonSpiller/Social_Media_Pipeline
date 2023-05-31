# %%
# import libraries
import pandas as pd, os, sys
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
stocks_df = pd.read_parquet('./data/transformed/stocks/stock_tickers_norm.parquet',
                            engine= 'pyarrow',
                            dtype_backend = 'pyarrow')
stocks_non_norm_df = pd.read_parquet('./data/extracted/merged/stock_tickers.parquet',
                            engine= 'pyarrow',
                            dtype_backend = 'pyarrow')
twitter_df = pd.read_parquet('./data/transformed/twitter/pivot_user_by_date_wkd_merge.parquet', 
                             engine= 'pyarrow',
                             dtype_backend = 'pyarrow')
today = date.today()
todays_twitter = twitter_df[twitter_df['date'] == today]
if( len(todays_twitter) > 0 ):
    
    # Merging historical twitter probabilities and ticker prices
    df_merge = pd.merge(stocks_df, twitter_df, how='inner', on='date').fillna(0)
    non_norm_merge_df = pd.merge(stocks_non_norm_df, twitter_df, how='inner', on='date').fillna(0)
    # Export twitter and stock merge
    df_to_parquet(df = df_merge, 
            folder = f'./data/transformed/merged', 
            file = f'/stocks_and_twitter.parquet')
    
    # Download Today's Test
    with open(os.path.normpath(os.getcwd() + './user_input/stock_tickers.xlsx'), 'rb') as f:
        ticker_df = pd.read_excel(f, sheet_name='ticker_sheet')
        ticker_df = ticker_df.where(pd.notnull(ticker_df), '')
        f.close()
    stock_str = " ".join(ticker_df.ticker_name)
    n = len(ticker_df.ticker_label) # how many tickers
    # Columns to rename
    column_rename  =(({'Datetime':'date'}) | dict(zip(ticker_df.ticker_name, ticker_df.ticker_label)))
    
    # Download minute prices for Stocks from today
    todays_stocks_df = yf.download(stock_str, period='1d', interval = '1m', progress=False)['Close'].reset_index().rename(columns=column_rename).fillna(0)
    # convert to pyarrow
    todays_stocks_df = todays_stocks_df.set_index('date')
    todays_stocks_df = todays_stocks_df.astype('float64[pyarrow]').tz_convert('UTC').reset_index().astype({"date":"timestamp[us][pyarrow]"})
    
    # merge todays stocks and twitter
    # todays_test = pd.merge(todays_stocks_df, todays_twitter, how='inner', on='date')
    todays_merge = pd.merge(todays_stocks_df, todays_twitter.drop(columns='date'), how='cross')
    merged_columns = list(ticker_df.ticker_label) + ['favorite_count', 'retweet_count']
    todays_test = normalize_columns_target(todays_merge.copy(), non_norm_merge_df.copy(), merged_columns)
    # Export latest test
    df_to_parquet(df = todays_test, folder = f'./data/transformed/merged', file = f'/todays_test.parquet')
    
    # Xnew = sm.add_constant(todays_test, has_constant='add')

    # model = {} # Model Build For Each index fund
    # print(f"date: { todays_test.index.date.max() }")
    # output = pd.DataFrame(columns=['index', 'prediction'])
    # for t in ticker_df.ticker_label:
    #     data_with_target = create_target(df_merge.copy(), day = 5, ticker = t)
    #     m = linear_model(data_with_target,split=0.20,summary = False)
    #     y_pred = m['lm'].predict(Xnew)
    #     model[t] = (y_pred, m)
    #     output = pd.concat([output, pd.DataFrame.from_records([(t, y_pred[0])], columns=['index', 'prediction'])])
        
    # pd.set_option('display.max_rows', 500)
    # display(output.sort_values(by=['prediction'], ascending=False))
else:
    print(f"Please Download Today's Tweets:\n")
# %%