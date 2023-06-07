# Load Libraries
import os, sys, pandas as pd, time
from facebook_scraper import *

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

# import local files
from extract_facebook_tools import *

# If credentials are needed
# access_credentials = pd.read_csv(f'./credentials/facebook.csv')
# access_token,app_token = access_credentials[0],access_credentials[1]

with open(os.path.normpath(os.getcwd() + '/user_input/facebook_users.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
facebook_groups = list(user_df.columns)
         
# %% [markdown]
# Download facebook posts
for group in facebook_groups:
    print(f"\n{group}:\n")
    # grab all user's from columns with user's
    users = list(user_df[group][user_df[group]!= ''])
    
    folder = f'./data/extracted/raw/facebook/{group}'
    options = {"comments": True, "reactors": True, "progress": True, "posts_per_page": 200}
    for user in users:
        posts = []
        for post in get_posts(account=f'{user}',
                              extra_info = True,
                              cookies=f"./credentials/facebook_cookies.txt",
                              pages=100,
                              options = options):
            time.sleep(20)
            posts.append(post)
        df  = pd.DataFrame(posts, 
                           engine='pyarrow',
                           dtype_backend = 'pyarrow')
        
        print(df)
        df_to_parquet(df=df, 
                      folder=folder, 
                      file=f'/{user}.parquet')
        print(f"")
print('Facebook user download complete')
# %%
