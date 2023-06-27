import yfinance as yf, pandas as pd, os

def download_historical_stocks(stocks_to_download, columns_to_rename, how_far_back, upto, index, file, folder, progress = False, period='1d', interval = '1d'):
    
    # Download closing prices for Stocks from date Range
    df = yf.download(stocks_to_download,
                                how_far_back,
                                upto,
                                period=period,
                                interval = interval,
                                keepna=True,
                                progress=progress)['Close'].reset_index().rename(columns=columns_to_rename).fillna(0)
    df = df.set_index(index).astype('float64[pyarrow]').reset_index()
    df[index] = df[index].dt.tz_localize('America/New_York').dt.tz_convert('America/New_York')
    # export original
    df_to_parquet(df = df, 
                folder = folder, 
                file = f'/{file}.parquet')
    return df

def normalize_historical_stocks(df, columns, file, folder):
    
    # Min Max normalize tickers
    for c in columns:
        df[c] = (df[c] - df[c].min()) / (df[c].max() - df[c].min()) 
    # export normalized
    df_to_parquet(df = df, 
            folder = folder, 
            file = f'/{file}.parquet')
    
def download_todays_stocks(stocks_to_download, columns_to_rename, index, file, folder, period='1d', interval='1m'):
    
    df = yf.download(stocks_to_download, 
                                   period=period, 
                                   interval = interval,
                                   keepna=True, 
                                   progress=False)['Close'].reset_index().rename(columns=columns_to_rename).fillna(0)
    # convert to pyarrow
    df = df.set_index(index).astype('float64[pyarrow]').reset_index()
    df[index] = df[index].dt.tz_convert('America/New_York')
    # export original
    df_to_parquet(df = df, 
            folder = folder, 
            file = f'/{file}.parquet')
    return df

def normalize_todays_stocks(df_today, df_historical, columns, file, folder):
    # Min Max normalize tickers based on historical stocks
    for c in columns:
        df_today[c] = (df_today[c] - df_historical[c].min()) / (df_historical[c].max() - df_historical[c].min())
    # export normalized
    df_to_parquet(df = df_today, 
            folder = folder, 
            file = f'/{file}.parquet')
    
def df_to_parquet(df, folder, file):
    """_summary_
        Save Dataframe as a parquet in a particular folder with specified file name
    Args:
        df (pandas): any pandas dataframe
        folder (string): folder location from source
        file (string): file to name parquet file
    """
    if not os.path.exists(folder):
        os.makedirs(folder)
    df.to_parquet(path= folder+file, index=False, engine='pyarrow')
    return