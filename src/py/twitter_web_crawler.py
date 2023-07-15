
# %%

# Import Libraries
import os, pandas as pd, tweepy, numpy as np, tweepy, glob, re, advertools, json
# import selenium and driver manager's
from selenium import webdriver
# from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common import exceptions
from bs4 import BeautifulSoup as bs
import time
from PIL import Image # Imaging Library Pillow
# from Screenshot import Screenshot_Clipping # selenium screenshot
import Screenshot

# %%

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
# from extract_twitter_tools import user_download, twitter_authentication, merge_tweets

# %%
# # Load Twitter Usernames   
# * Accounts may be privated or removed and change ability to download
# * No two users can have the same id
with open(os.path.normpath(os.getcwd() + '/user_input/twitter_users.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
twitter_groups = list(user_df.columns)

credentials = pd.read_csv(f'./credentials/twitter.csv', header=0)
email,password = credentials.email[0],credentials.password[0]


# %%
print("Setting Up Browser Configurations")
LOGIN_URL = 'https://www.twitter.com/login.php'
options = webdriver.ChromeOptions()
# options.add_argument('--headless')  # runs browser in headless mode
options.add_argument('--no-sandbox')
options.add_argument("--disable-dev-shm-usage")
options.add_argument('--ignore-certificate-errors')
# options.add_argument('--disable-gpu')
options.add_argument('--log-level=3')
options.add_argument('--disable-notifications')
options.add_argument('--disable-popup-blocking')
browser = webdriver.Chrome(executable_path=ChromeDriverManager(path = r"./drivers").install(),
                            options=options)
agent = browser.execute_script("return navigator.userAgent")
width,height = 784,880
browser.set_window_position(784, 0)
browser.set_window_size(width=width, height=height)
browser.get(LOGIN_URL)
window_size = browser.get_window_size()

# %%
print("Logging Into Twitter")
wait = WebDriverWait(browser, 30)

# %%
print("Email Login")
username_input = wait.until(EC.visibility_of_element_located((By.NAME, "text")))
username_input.send_keys(email)
wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@role='button'][contains(.,'Next')]"))).click()

# %%
print("Phone Login")
try:
    phone = WebDriverWait(browser, 2).until(EC.visibility_of_element_located((By.NAME, "text")))
    phone.send_keys('3604870735')
    phone.send_keys(Keys.RETURN)
except exceptions.TimeoutException :
    print("No phone Number Required")
    pass
# %%
print("Password Login")
password_input = wait.until(EC.visibility_of_element_located((By.NAME, "password")))
password_input.send_keys(password)
password_input.send_keys(Keys.RETURN)
time.sleep(4)
print("Done Logging In")

# %%
print("Begin User Page Scape")
# loop through user's
# for group in user_df:
#     for user in user_df[group]:
#         print(user)
user = user_df.iloc[0][0]
print(f'user:{user}\n')
browser.get(f'https://www.twitter.com/{user}')

# %%
# print('Screenshot Test:')
# Screenshot.full_screenshot(driver = browser, save_path='./images/screenshots', image_name='screenshot.png',load_wait_time=1)
# Screenshot.get_element()

# %%
time.sleep(1)
SCROLL_PAUSE_TIME,posts = 4,[]
browser.execute_script(f"window.scrollTo(0, 0)")
# browser.delete_all_cookies()
# 3 variations [scrollBy (x-coord, y-coord), scrolTo (x-coord, y-coord), scroll (x-coord, y-coord)]
while True:
    last_position = browser.execute_script(f"return window.scrollY")
    try:
        page_text = browser.find_elements(By.XPATH, '//div[@data-testid="tweetText"]')
        page_metrics = browser.find_elements(By.XPATH, "//*[(contains(@aria-label, 'likes') and contains(@aria-label, 'views')) or (contains(@aria-label, 'likes') and contains(@aria-label, 'Retweets')) or (contains(@aria-label, 'views') and contains(@aria-label, 'Retweets'))]")
        timestamps = browser.find_elements(By.XPATH, "//time")
        replies,likes,views,retweets = 0,0,0,0
        for txt,metrics,ts in zip(page_text,page_metrics,timestamps):
                created_at = pd.to_datetime(ts.get_attribute('datetime'))
                text = txt.get_attribute('innerText').encode(encoding='utf-8').decode(encoding='utf-8')
                metric = metrics.get_attribute('aria-label')
                metric_num = re.findall('[\d]+', metric)
                metric_string = re.findall('[a-zA-Z]+', metric)
                for s,num in zip(metric_string, metric_num):
                    if(s == 'reply' or s =='replies'):
                        replies= num
                    if(s == 'Retweets' or s =='Retweet'):
                        retweets = num
                    if(s == 'likes' or s =='likes'):
                        likes = num
                    if(s == 'views' or s =='view'):
                        views = num
                post = [created_at, text, metric, replies, retweets, likes, views]
                if(post not in posts):
                    posts.append(post)
                browser.execute_script(f"window.scrollBy(0, 300)")
    except Exception as e:
        browser.execute_script(f"window.scrollBy(0, 400)")
        continue
    position = browser.execute_script(f"return window.scrollY")
    if( position == last_position):
        break
df = pd.DataFrame(data = posts, columns = ['created_at','text', 'metrics', 'replies', 'retweets', 'likes', 'views'])
df
# print(texts, posts, metric, height, sep='\n')
# %%
# SCROLL_PAUSE_TIME,texts = 4,[]
# browser.execute_script(f"window.scrollTo(0, 0)")
# while True:
#     browser.execute_script("window.scrollBy(0,400)", "")
# last_height = browser.execute_script("window.scrollTo(0, document.body.scrollHeight)")
# while True:
#     browser.execute_script("window.scrollTo(0, document.body.scrollHeight)")
#     tweets = browser.find_elements(By.XPATH, '//div[@data-testid="tweetText"]')
#     for i in tweets:
#         print(i.get_attribute('innerText'))      
        
#     time.sleep(SCROLL_PAUSE_TIME)
#     new_height = browser.execute_script("return document.body.scrollHeight")
#     if new_height == last_height:
#         break
#     last_height = new_height
# browser.quit()
# %%