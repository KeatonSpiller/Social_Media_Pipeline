# %%
# Load Libraries
import os, sys, pandas as pd, time
from facebook_scraper import *
import facebook
import browser_cookie3
# from facebook import user_posts

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
# %%
# If credentials are needed
# access_credentials = pd.read_csv(f'./credentials/facebook.csv', header=0)
# access_token,app_token = access_credentials.access_token,access_credentials.app_token
# graph = facebook.GraphAPI(access_token=access_token)

with open(os.path.normpath(os.getcwd() + '/user_input/facebook_users.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
facebook_groups = list(user_df.columns)

# %% [markdown]
# cookies (automated or maunal)
cookies = browser_cookie3.chrome(domain_name='.facebook.com')
# cookies=f"./credentials/facebook_cookies.txt"

# %%
# Download facebook posts
for group in facebook_groups:
    print(f"\n{group}:\n")
    # grab all user's from columns with user's
    users = list(user_df[group][user_df[group]!= ''])
    
    folder = f'./data/extracted/raw/facebook/{group}'
    options = {"comments": True, "reactors": True,\
               'HQ_images': False,'allow_extra_requests': False}
    
    start_url = None
    def handle_pagination_url(url):
        global start_url
        start_url = url
    # pages=None
    for user in users:
        print(f"{user}\n")
        try:
            posts = []
            for post in get_posts(account=f'{user}',
                                extra_info = True,
                                cookies=cookies,
                                page_limit=None, 
                                start_url=start_url,
                                request_url_callback=handle_pagination_url):
                time.sleep(10)
                posts.append(post)
            df  = pd.DataFrame(posts)
            
            print(df)
            df_to_parquet(df=df, 
                        folder=folder, 
                        file=f'/{user}.parquet')
            print(f"")
        
        except exceptions.TemporarilyBanned:
            print("Temporarily banned, sleeping for 10m")
            time.sleep(600)
print('Facebook user download complete')

# %%
