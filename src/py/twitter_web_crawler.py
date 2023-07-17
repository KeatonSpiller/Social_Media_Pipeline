
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
# options.add_argument('--disable-extensions')
options.add_extension(f'./drivers/NoBufferCRX0.4.4.crx') # Stop video's from auto playing
options.add_argument('--blink-settings=imagesEnabled=false')
options.add_argument('--disable-gpu')
# options.add_argument('--log-level=3')
options.add_argument('--disable-notifications')
options.add_argument('--disable-popup-blocking')
# remove password saving
prefs = {"credentials_enable_service": False,
     "profile.password_manager_enabled": False}
options.add_experimental_option("prefs", prefs)
# fix "Failed to load resource" (status 403/401)
# options.add_argument('--disable-blink-features=AutomationControlled')
options.add_experimental_option('useAutomationExtension', False)
options.add_experimental_option("excludeSwitches", ["enable-automation"])

browser = webdriver.Chrome(executable_path=ChromeDriverManager(path = r"./drivers").install(),
                            options=options,
                            keep_alive=False)
agent = browser.execute_script("return navigator.userAgent")
browser.set_window_size(width=782, height=871) #{'width': 782, 'height': 871}
browser.set_window_position(x=761, y=0) #{'x': 761, 'y': 0}
browser.get(LOGIN_URL)
window_size, window_position = browser.get_window_size(), browser.get_window_position()

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
user = user_df.iloc[0][1]
print(f'user:{user}\n')
browser.get(f'https://www.twitter.com/{user}')
user_name = WebDriverWait(browser, 20).until(EC.visibility_of_element_located((By.XPATH, '//div[@data-testid="UserName"]'))).get_attribute("innerText")

# %%
# 3 variations (scrollBy, scrolTo, scroll)
# print(f'{user_name}\n Extracting')
# browser.delete_all_cookies()
browser.execute_script(f"window.scrollTo(0, 0)") 
SCROLL_PAUSE_TIME,posts,continue_scrolling,repetitive_scroll = 4,[],True,0
metric_var = ['likes', 'views', 'Retweets', 'replies', 'like', 'view', 'Retweet', 'reply']
metric_conditional = ''
for i in range(0, len(metric_var)):
    for j in range(i+1, len(metric_var)-1):
        if ( i != j):
            var1= metric_var[i]
            var2=metric_var[j]
            metric_conditional += f"(contains(@aria-label, '{var1}') and contains(@aria-label, '{var2}')) or "
while(continue_scrolling==True or repetitive_scroll <= 2):
    last_position = browser.execute_script(f"return window.scrollY")
    try:
        repetitive_scroll = 0
        # How much to sleep to avoid breaking the program
        time.sleep(0.25)
        page_text = WebDriverWait(browser, 2).until(EC.presence_of_all_elements_located((By.XPATH, '//div[@data-testid="tweetText"]')))
        conditional_metric_string = "[metric_conditional]"
        page_metrics = WebDriverWait(browser, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//*[(contains(@aria-label, 'likes') and contains(@aria-label, 'views')) or (contains(@aria-label, 'likes') and contains(@aria-label, 'Retweets')) or (contains(@aria-label, 'views') and contains(@aria-label, 'Retweets')) or (contains(@aria-label, 'views') and contains(@aria-label, 'reply')) or (contains(@aria-label, 'views') and contains(@aria-label, 'reply'))]")))
        timestamps = WebDriverWait(browser, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//time")))
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
            if(browser.execute_script(f"return window.scrollY") == browser.execute_script(f"return document.body.scrollHeight")):
                time.sleep(4)
            browser.execute_script(f"window.scrollBy(0, 500)")
    except Exception as e:
        time.sleep(0.25)
        browser.execute_script(f"window.scrollBy(0, 500)")
        repetitive_scroll +=1
        pass
    position = browser.execute_script(f"return window.scrollY")
    if( position == last_position ):
        print(position, last_position)
        continue_scrolling=False
        break
        # browser.quit()
 
df = pd.DataFrame(data = posts, columns = ['created_at','text', 'metrics', 'replies', 'retweets', 'likes', 'views'])
df = df.drop_duplicates(subset=['created_at','text'], keep='last').sort_values('created_at', ascending=False).reset_index(drop=True)
df
# %%