# %%
# Import Libraries
import os, pandas as pd, numpy as np, getpass, datetime
from sqlalchemy import create_engine

# %% [markdown]
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
    except Exception as e:
        print(f"cwd: {os.getcwd()}", sep = '\n')
        print(f"{e}\n:Please start current working directory from {top_level_folder}")
        
# %%
# Import Local Scripts
from src.py.load.load_tools import *       
        
# %%
# Access Root credentials?
file = './credentials/mysql.parquet'
new_credentials = False
if(new_credentials):
    create_credentials(file)
    
# %%
# Read in Credentials
df_credentials = pd.read_parquet(file, engine='pyarrow')
# Ask for Password to root server
user_password = str(getpass.getpass("Verify Password: "))
# Argon2 ID Hash verify password
verification = verify_password(df_credentials.password[0], user_password)

# %%
if(verification == True):
    # replace hash with temporary password string
    df_credentials.password = user_password
    
    # Pull credentials from DataFrame
    user = df_credentials.user[0]
    password = df_credentials.password[0]
    host = df_credentials.host[0]
    port = df_credentials.port[0]
    
    # Create Database
    database = 'socialmedia'
    db_query = f"""CREATE DATABASE IF NOT EXISTS {database};
    """
    mysql_execute(credentials=df_credentials, type="CREATE_DB", query=db_query)
    
    # Connect MYSQL with sqlalchemy
    url = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(url=url, echo=False)

    # Read in Raw Tweets
    twitter_df = pd.read_parquet('./data/extracted/merged/all_twitter.parquet').astype(dataframe_astypes())
    
    # Rename schema for MYSQL
    twitter_df.rename(columns = {"id":"twitter_id",
                                 "user":"twitter_user",
                                 "group":"twitter_group",
                                 "url":"tweet_url",
                                 "links":"all_urls",
                                 "text":"raw_text"},
                      inplace = True)

    fact_table = "rawtweets_fact"
    user_table = "user_dim"
    date_table = "date_dim"
    
    date_df = pd.DataFrame()
    date_df['timestamp'] = twitter_df.created_at.dt.tz_convert('UTC')
    date_df["date"] = date_df.timestamp.dt.date
    date_df["time"] = date_df.timestamp.dt.time
    date_df["year"] = date_df.timestamp.dt.year
    date_df["month"] = date_df.timestamp.dt.month
    date_df["day"] = date_df.timestamp.dt.day
    date_df["hour"] = date_df.timestamp.dt.hour
    date_df["minute"] = date_df.timestamp.dt.minute
    date_df["second"] = date_df.timestamp.dt.second
    date_df["timezone"] = date_df.timestamp.dt.tz
    twitter_df.to_sql(name=date_table,
                      con=engine, 
                      if_exists = 'replace', 
                      index=False,
                      chunksize=5000, 
                      method='multi')
    with engine.connect() as con:
        con.execute(f'ALTER TABLE {date_table} ADD PRIMARY KEY (id);')
        
    # ALTER TABLE {date_table} ADD FOREIGN KEY (PersonID) REFERENCES Persons(PersonID)
        
    # # Insert df into table
    # twitter_df.to_sql(name=table,
    #                   con=engine, 
    #                   if_exists = 'replace', 
    #                   index=False,
    #                   chunksize=5000, 
    #                   method='multi')
    
# %%
