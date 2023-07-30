# %%
# *** Import Libraries ***
# 1. Standard library imports. ( https://docs.python.org/3/library/index.html )
import os 
import time

# 2. Related third party imports.
import pandas as pd 
import numpy as np

# *** Change Root Directory to top level folder ***
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
        
# *** Load Custom Functions ***
# 3. Local application/library specific imports.
# Explicit Modules
from twitter_web_crawler_tools import load_users, parallel_extract_twitter

if __name__ == '__main__':
    user_df = load_users()
    maximum = np.Infinity
    document = 'document.documentElement.scrollHeight'
    element = 'document.documentElement.scrollHeight'
    client = 'document.documentElement.clientHeight'
    offset = 'document.documentElement.offsetHeight'
    body = 'return document.body.scrollHeight'
    page = 800
    midpage = 400
    flow_login = 'https://twitter.com/i/flow/login' # standard: Google Chrome agent login
    login = 'https://twitter.com/login' # bot: GoogleBot 2 Desktop agent
    parallel_extract_twitter(user_df, 
                             folder = f'./data/extracted/raw/twitter',
                             login_url=login,
                             agent ='standard',
                             headless = False, 
                             full_screen = False, 
                             scroll_loops = maximum, 
                             howfar = '1970-01-01',
                             scroll_by = page,
                             sleep = 4,
                             cookies = None,
                             debug = False)
