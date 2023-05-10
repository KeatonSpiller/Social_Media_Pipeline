import os, glob, pandas as pd, getpass, argon2, sshtunnel

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

# Import Local Files
from src.py.load.load_tools import *       
# %% [markdown]
        
# %%
# Create credentials?
file = './credentials/mysql.parquet'
# Temporarily Don't need root credentials changed
if(False):
    create_credentials(file)
df_credentials = pd.read_parquet(file, engine='pyarrow')
user_password = str(getpass.getpass("Verify Password: "))
verification = verify_password(df_credentials.password[0], user_password)

# If argon2 ID has verified the correct root passphrase then continue
if(verification == True):
    df_credentials.password = user_password
    
    # %%
    # Create Database
    database = 'socialmedia'
    create_database_query = f"""CREATE DATABASE IF NOT EXISTS {database};
    """
    mysql_execute(credentials=df_credentials, type="CREATE_DB", query=create_database_query)
    
    # %%
    # Create Table Schema
    twitter_df = pd.read_parquet('./data/extracted/merged/all_twitter.parquet')
    print(twitter_df.dtypes)
    table_query = f"""CREATE TABLE IF NOT EXISTS RawTweets (
                    id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    twitter_id INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    entry_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    user VARCHAR(200) NOT NULL,
                    user_group VARCHAR(200) NOT NULL,
                    tweet_url VARCHAR(200) NOT NULL,
                    favorite_count INT NOT NULL,
                    retweet_count INT NOT NULL,
                    hashtags VARCHAR(200) NOT NULL,
                    emojis VARCHAR(200) NOT NULL,
                    emoji_text VARCHAR(200) NOT NULL,
                    usernames VARCHAR(200) NOT NULL,
                    links VARCHAR(200) NOT NULL,
                    tweet VARCHAR(200) NOT NULL);
    """
    mysql_execute(credentials=df_credentials, query=table_query, type="default", db=database)
    
    # %%
    print(twitter_df.head(5))
    # # Insert Values into Tables
    print(twitter_df.head(5))
    # print(f"Creating database:")
    # query = """INSERT INTO clients(
    #         firstname, lastname, email)
    #         VALUES ("John", "Lennon", "johnlennon@gmail.com")"""
    # mysql_execute(query)
    # print("Query executed!")
    # %%
    
