# %%
# Load Libraries
import os, sys, pandas as pd
# from facebook import user_posts
from linkedin_scraper import Person, actions, Company
from selenium import webdriver

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

# %%
# If credentials are needed
# access_credentials = pd.read_csv(f'./credentials/facebook.csv', header=0)
# access_token,app_token = access_credentials.access_token,access_credentials.app_token
# graph = facebook.GraphAPI(access_token=access_token)

# with open(os.path.normpath(os.getcwd() + '/user_input/facebook_users.xlsx'), 'rb') as f:
#     user_df = pd.read_excel(f, sheet_name='user_names')
#     user_df = user_df.where(pd.notnull(user_df), '')
#     f.close()
# facebook_groups = list(user_df.columns)
credentials = pd.read_csv(f'./credentials/linkedin.csv', header=0)
email,password = str(credentials.email[0]), str(credentials.password[0])
user = 'keatonspiller'
company = 'cnn'
driver = webdriver.Chrome(executable_path=f'./credentials/chromedriver.exe')
actions.login(driver, email, password) # if email and password isnt given, it'll prompt in terminal
# person = Person(f"https://www.linkedin.com/in/{user}", driver=driver)
# df = person.scrape(close_on_complete=True)
company = Company(f"https://ca.linkedin.com/company/{company}", driver=driver)
df = company.scrape(close_on_complete=True)
# %%
