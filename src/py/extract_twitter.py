# %% [markdown]
## Parse Data
#       Summary Overview
#   - Using Tweepy API to parse data from Twitter API
#   - 200 chunks of tweets for 3200 tweets for each User in user_input
#   - strips emoji's, hashtags, usernames, and website links into seperate columns

# %% [markdown]
## Import Libraries
import os, pandas as pd, openpyxl

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
  
# %% [markdown]
## Load Custom Functions
from extract_twitter_tools import user_download, twitter_authentication, merge_tweets

# %% [markdown]
# # Twitter API Credentials
# Read in keys from a csv file
authentication_path = os.path.abspath('./credentials/twitter.csv')
api = twitter_authentication(authentication_path)

# %% [markdown]
# # Load Twitter Usernames   
# * Accounts may be privated or removed and change ability to download
# * No two users can have the same id
with open(os.path.normpath(os.getcwd() + '/user_input/twitter_users.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
twitter_groups = list(user_df.columns)
         
# %% [markdown]
# ## Download Tweets
#     * 3200 limit, adds to previously downloaded files
for group in twitter_groups:
    print(f"\n{group}:\n")
    # grab all user's from columns with user's
    users = list(user_df[group][user_df[group]!= ''])
    
    folder = f'./data/extracted/raw/twitter/{group}'
    user_download(api=api, 
                  user_list=users, 
                  group=group, 
                  folder=folder, 
                  display='full')
    print(f"")
print('Twitter user download complete')

# %%
# Merge all Tweets Together
all_tweets_df = merge_tweets(twitter_groups,
                            folder = f"./data/extracted/raw/twitter/",
                            merge = f"./data/extracted/merged/twitter/groups/",
                            all_merge = f"./data/extracted/merged/twitter/")
print(f"size of merged tweets file: {all_tweets_df.shape}\n")
# %%
