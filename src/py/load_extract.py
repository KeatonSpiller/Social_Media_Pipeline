# %%
# Import Libraries
import os, pandas as pd, getpass
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
from load_tools import create_credentials, argon_hash, verify_password, mysql_execute, mysql_connect,  timestamp_split_df, dataframe_astypes    
        
# %%
# Access Root credentials
file = './credentials/mysql.parquet'
# create_credentials(file)
df_credentials = pd.read_parquet(file, engine='pyarrow')

# %%
# Pull credentials from DataFrame
user = df_credentials.user[0]
password = df_credentials.password[0]
host = df_credentials.host[0]
port = df_credentials.port[0]

# Create Database
database = 'socialmedia'
db_query = f"""CREATE DATABASE IF NOT EXISTS {database};"""
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
# Create user Dimension Table - Organize by unique user's and Distinct mentions of users
user_table = "user_dim"
user_df = twitter_df.loc[:, ["twitter_user", "twitter_group", "mentioned_users"]].fillna("")
user_df=user_df.groupby(["twitter_user", "twitter_group"]).agg({'mentioned_users': lambda x: " ".join(set(x))}).reset_index()
create_user_table = f"""CREATE TABLE IF NOT EXISTS {user_table} (
                    ID INT PRIMARY KEY AUTO_INCREMENT,
                    twitter_user varchar(200),
                    twitter_group text,
                    mentioned_users text,
                    CONSTRAINT no_duplicates UNIQUE (ID,twitter_user));
                    """
engine.execute(create_user_table)
user_df.to_sql( name=user_table,
                con=engine, 
                if_exists = 'append', 
                index=False,
                chunksize=10000, 
                method='multi')
# %%
# Create Date Dimension Table - organize by timestamps
date_table = "date_dim"
date_df = timestamp_split_df(pd.Series(twitter_df.created_at))
date_df.insert(0, 'twitter_id', twitter_df.twitter_id)
create_date_table = f"""CREATE TABLE IF NOT EXISTS {user_table} (
                    ID INT PRIMARY KEY AUTO_INCREMENT,
                    twitter_id bigint,
                    `timestamp` timestamp,
                    timezone text,
                    `date` date,
                    `time` time,
                    year int,
                    month int,
                    day int,
                    hour int,
                    minute int,
                    second int,
                    CONSTRAINT no_duplicates UNIQUE (ID,twitter_id));
                    """
engine.execute(create_date_table)
date_df.to_sql(name=date_table,
                    con=engine, 
                    if_exists = 'append', 
                    index=False,
                    chunksize=10000, 
                    method='multi')
    
# %%
# Create Fact Table Last...
fact_table = "rawtweets_fact"
fact_df = twitter_df.loc[:, ["twitter_id", 'raw_text', "tweet_url", "favorite_count", "retweet_count", 'hashtags', 'emojis', 'emoji_text', 'all_urls']]
    
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
                    FOREIGN KEY (UserID_FK) REFERENCES {user_table}(ID),
                    CONSTRAINT no_duplicates UNIQUE (ID,twitter_id));
                    """
engine.execute(create_sql_table)

# Read in Dimension Tables to extract foreign keys to associate with the fact table
with engine.connect() as con:
    readin_date_df = pd.read_sql_table(f'{date_table}', con)
    readin_user_df = pd.read_sql_table(f'{user_table}', con)
user_merge = pd.merge(twitter_df, readin_user_df, how='left', on=["twitter_user"])
fact_df.insert(0,'userID_FK',user_merge.ID)

fact_df.to_sql( name=fact_table,
                con=engine,
                if_exists = 'append', 
                index=False,
                chunksize=10000,
                method='multi') 
# %%

