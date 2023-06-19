# %%
# Load Libraries
import os, sys, pandas as pd, time
from facebook_scraper import *
import facebook
import browser_cookie3

# import selenium and driver manager's
from selenium import webdriver
# from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup as bs
import time

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
access_credentials = pd.read_csv(f'./credentials/facebook.csv', header=0)
access_token,app_token = access_credentials.access_token,access_credentials.app_token
email,password = access_credentials.email, access_credentials.password

with open(os.path.normpath(os.getcwd() + '/user_input/facebook_users.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
facebook_groups = list(user_df.columns)

# %% [markdown]
# cookies (automated or maunal)
# cookies = browser_cookie3.chrome(domain_name='.facebook.com')
# cookies=f"./credentials/facebook_cookies.txt"

# %%
# Download facebook posts with facebook-scraper
# for group in facebook_groups:
#     print(f"\n{group}:\n")
#     # grab all user's from columns with user's
#     users = list(user_df[group][user_df[group]!= ''])
    
#     folder = f'./data/extracted/raw/facebook/{group}'
#     options = {"comments": True, "reactors": True,\
#                'HQ_images': False,'allow_extra_requests': False}
    
#     start_url = None
#     def handle_pagination_url(url):
#         global start_url
#         start_url = url
#     # pages=None
#     for user in users:
#         print(f"{user}\n")
#         try:
#             posts = []
#             for post in get_posts(account=f'{user}',
#                                 extra_info = True,
#                                 cookies=cookies,
#                                 page_limit=None, 
#                                 start_url=start_url,
#                                 request_url_callback=handle_pagination_url):
#                 time.sleep(10)
#                 posts.append(post)
#             df  = pd.DataFrame(posts)
            
#             print(df)
#             df_to_parquet(df=df, 
#                         folder=folder, 
#                         file=f'/{user}.parquet')
#             print(f"")
        
#         except exceptions.TemporarilyBanned:
#             print("Temporarily banned, sleeping for 10m")
#             time.sleep(600)
# print('Facebook user download complete')

# %%
# Access Facebook API
# graph = facebook.GraphAPI(access_token=access_token)
# events = graph.request(f'/search?q=Poetry&type=event&limit=10000')
# print(events)
# %%
# https://medium.com/nerd-for-tech/collecting-public-data-from-facebook-using-selenium-and-beautiful-soup-f0f918971000
# using selenium to login
# def Facebook_Login(email='', password = '', browser='Chrome'):
LOGIN_URL = 'https://www.facebook.com/login.php'
options = webdriver.ChromeOptions()
options.add_argument('--disable-notifications')
options.add_argument("--disable-infobars")
options.add_argument("start-minimized")
options.add_argument("--disable-extensions")
browser = webdriver.Chrome(executable_path=ChromeDriverManager(path = r"./drivers").install(),
                            options=options)
width,height = 960,540
browser.set_window_size(width, height)
browser.get(LOGIN_URL)
wait = WebDriverWait(browser, 30)
email_element = wait.until(EC.visibility_of_element_located((By.NAME, 'email')))
# email_element = driver.find_element(By.ID, 'email')
email_element.send_keys(email)

# pass_element = driver.find_element(By.ID, 'pass')
pass_element = wait.until(EC.visibility_of_element_located((By.NAME, 'pass')))
pass_element.send_keys(password)

login_button = wait.until(EC.visibility_of_element_located((By.NAME, 'login')))
# login_button = browser.find_element(By.ID, 'loginbutton')
# login_button.click() # Send mouse click
login_button.send_keys(Keys.RETURN)

# %%
# Facebook Search Bar
search = browser.find_element(By.CSS_SELECTOR, "[aria-label='Search Facebook']")
search.click()
# %%
profile = 'Nintendo of America'
search.send_keys(f'{profile}')
# %%
# posts_from = browser.find_element(By.XPATH, "//input[contains(@aria-label, 'Posts From')]").click()
search.send_keys(Keys.RETURN)
time.sleep(3)
# %%
first_result = browser.find_element(By.XPATH, f"//a[contains(@aria-label, '{profile}')]").click()

# %%
# Click See more if exists
def see_more_click():
    try:
        browser.find_element(By.XPATH, "//div[contains(text(), 'See more')]").click()
    except Exception as e:
        print(e)

# %%
# start = browser.find_element(By.CSS_SELECTOR, "[style='text-align: start;']")
# print(start.text)
# %%


page_source = browser.page_source
current_url = browser.current_url
soup = bs(page_source,features="html.parser")

# %%
import re
# %%
post = browser.find_elements(By.XPATH, "//div[contains(@class, 'xdj266r x11i5rnm xat24cr x1mh8g0r x1vvkbs x126k92a')]")
text = []
for i,element in enumerate(post):
    s = element.text
    if('See more' in s):
        see_more_click()
        s = element.text
    print(f'post #{i+1}\n')
    print (f'{element.text}\n')
    
    next_sibling = browser.execute_script("""
    return arguments[0].nextElementSibling
    """, element)
    if(next_sibling is not None):
        print(f"{next_sibling.text}\n")
    
# %%
# %%

# %%
