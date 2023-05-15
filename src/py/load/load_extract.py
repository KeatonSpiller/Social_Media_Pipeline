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
    
    # Simplify the dataframe for test purposes
    twitter_df = twitter_df.iloc[0:100, :]
    
    # Rename dataframe schema for MYSQL
    twitter_df.rename(columns = {"id":"twitter_id",
                                 "user":"twitter_user",
                                 "group":"twitter_group",
                                 "url":"tweet_url",
                                 "links":"all_urls",
                                 "text":"raw_text",
                                 "usernames":"mentioned_users"},
                      inplace = True)
    
    # %%
    # Create user Dimension Table
    # - Organize by unique user's and Distinct mentions of users
    user_table = "user_dim"
    user_df = twitter_df.loc[:, ["twitter_user", "twitter_group", "mentioned_users"]].fillna("")
    user_df=user_df.groupby(["twitter_user", "twitter_group"]).agg({'mentioned_users': lambda x: " ".join(set(x))}).reset_index()
    user_df.to_sql( name=user_table,
                    con=engine, 
                    if_exists = 'replace', 
                    index=False,
                    chunksize=10000, 
                    method='multi')
    with engine.connect() as con:
        con.execute(f'ALTER TABLE {user_table} ADD ID int NOT NULL AUTO_INCREMENT primary key FIRST')
    # %%
     # Create Date Dimension Table
    # - Organize by User's that tweeted on the same date
    date_table = "date_dim"
    date_df = timestamp_split_df(pd.Series(twitter_df.created_at))
    date_df.insert(0, 'twitter_id', twitter_df.twitter_id)
    print(date_df)
    date_df.to_sql(name=date_table,
                      con=engine, 
                      if_exists = 'replace', 
                      index=False,
                      chunksize=10000, 
                      method='multi')
    with engine.connect() as con:
        con.execute(f'ALTER TABLE {date_table} ADD ID int NOT NULL AUTO_INCREMENT primary key FIRST')
        
    # %%
    # Create Fact Table Last...
    fact_table = "rawtweets_fact"
    fact_df = twitter_df.loc[:, ["twitter_id", 'raw_text', "tweet_url", "favorite_count", "retweet_count", 'hashtags', 'emojis', 'emoji_text', 'all_urls']]
        
    # Drop Previous Table
    with engine.connect() as con:
        con.execute(f'DROP TABLE IF EXISTS {fact_table} ')
    # %%
    # Create Schema for Fact Table
    create_sql_table = f"""CREATE TABLE IF NOT EXISTS {fact_table} (
                        ID INT PRIMARY KEY AUTO_INCREMENT,
                        UserID_FK INT,
                        twitter_id bigint,
                        raw_text text,
                        tweet_url text,
                        favorite_count bigint,
                        retweet_count bigint,
                        hashtags text,
                        emojis text,
                        emoji_text text,
                        all_urls text,
                        FOREIGN KEY (UserID_FK) REFERENCES {user_table}(ID));
                        """
    engine.execute(create_sql_table)
    
    # Read in Dimension Tables to extract foreign keys to associate with the fact table
    with engine.connect() as con:
        readin_date_df = pd.read_sql_table(f'{date_table}', con)
        readin_user_df = pd.read_sql_table(f'{user_table}', con)
    user_merge = pd.merge(twitter_df, readin_user_df, how='left', on=["twitter_user"])
    fact_df.insert(0,'userID_FK',user_merge.ID)
    date_merge = pd.merge(twitter_df, readin_date_df, how='left', on=['twitter_id'])
    fact_df.insert(0, 'ID', date_merge.ID)
    
    fact_df.to_sql( name=fact_table,
                    con=engine,
                    if_exists = 'append', 
                    index=False,
                    chunksize=10000,
                    method='multi') 
# %%

