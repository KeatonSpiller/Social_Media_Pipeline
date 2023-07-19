
# %%

# Import Libraries
import os, pandas as pd, tweepy, numpy as np, tweepy, glob, re, advertools, json
# import selenium and driver manager's
from selenium import webdriver
webdriver.Chrome
# from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common import exceptions
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
import time
import threading
import multiprocessing
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

def twitter_login(browser, email, password, phone):
    
    LOGIN_URL = 'https://www.twitter.com/login.php'
    browser.get(LOGIN_URL)
    print("Logging Into Twitter")
    wait = WebDriverWait(browser, 30)
    try:
        print("Email Login")
        username_input = wait.until(EC.visibility_of_element_located((By.NAME, "text")))
        username_input.send_keys(str(email))
        wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@role='button'][contains(.,'Next')]"))).click()
    except Exception as e:
        print("failed to login With Email")
    try:
        print("Phone Login")
        phone_input = WebDriverWait(browser, 4).until(EC.visibility_of_element_located((By.NAME, "text")))
        phone_input.send_keys(str(phone))
        phone_input.send_keys(Keys.RETURN)
    except exceptions.TimeoutException :
        print("No phone Number Required")
    try:
        print("Password Login")
        password_input = wait.until(EC.visibility_of_element_located((By.NAME, "password")))
        password_input.send_keys(str(password))
        password_input.send_keys(Keys.RETURN)
        time.sleep(4)
    except Exception as e:
        print("failed to login With Password")
    print("Done Logging In")

# %%            
def extract_twitter(browser, user):
    metric_var = ['likes', 'views', 'Retweets', 'replies', 'like', 'view', 'Retweet', 'reply']
    posts, metric_conditional, continue_scrolling, repetitive_scroll = [], '', True, 0
    
    browser.get(f'https://www.twitter.com/{user}')
    try:
        user_name = WebDriverWait(browser, 20).until(EC.visibility_of_element_located((By.XPATH, '//div[@data-testid="UserName"]'))).get_attribute("innerText")
        print(f'{user_name}\nExtracting')
    except Exception as e:
        print(f"Incorrect username {user}")
        pass
    browser.execute_script(f"window.scrollTo(0, 0)") # 3 variations (scrollBy, scrolTo, scroll)
    
    for i in range(0, len(metric_var)):
        for j in range(i+1, len(metric_var)-1):
            if ( i != j):
                var1= metric_var[i]
                var2=metric_var[j]
                metric_conditional += f"(contains(@aria-label, '{var1}') and contains(@aria-label, '{var2}')) or "
    metric_conditional_string = f"//*[{metric_conditional}"[0:-4] +"]"
    # for i in range(0,4):
    while(continue_scrolling==True or repetitive_scroll <= 2):
        last_position = browser.execute_script(f"return window.scrollY")
        try:
            repetitive_scroll = 0
            # How much to sleep to avoid breaking the program
            time.sleep(2)
            page_text = WebDriverWait(browser, 2).until(EC.presence_of_all_elements_located((By.XPATH, '//div[@data-testid="tweetText"]')))
            page_metrics = WebDriverWait(browser, 2).until(EC.presence_of_all_elements_located((By.XPATH, metric_conditional_string)))
            page_timestamps = WebDriverWait(browser, 2).until(EC.presence_of_all_elements_located((By.XPATH, "//time")))
            page_links = WebDriverWait(browser, 2).until(EC.presence_of_all_elements_located((By.XPATH, f"//a[contains(@href, '/{user}/status/') and not (contains(@href, 'analytics'))]")))
            ## page_source = bs(browser.page_source,'lxml')
            
            replies,likes,views,retweets = 0,0,0,0
            for txt,metrics,ts,links in zip(page_text,page_metrics,page_timestamps,page_links):
                
                text = txt.get_attribute('innerText').encode(encoding='utf-8').decode(encoding='utf-8')
                metric = metrics.get_attribute('aria-label')
                metric_num = re.findall('[\d]+', metric)
                metric_string = re.findall('[a-zA-Z]+', metric)
                created_at = pd.to_datetime(ts.get_attribute('datetime'))
                url = links.get_attribute('href')
                id = url.split("/")[-1]
                        
                for s,num in zip(metric_string, metric_num):
                    if(s == 'reply' or s =='replies'):
                        replies= num
                    if(s == 'Retweets' or s =='Retweet'):
                        retweets = num
                    if(s == 'like' or s =='likes'):
                        likes = num
                    if(s == 'views' or s =='view'):
                        views = num
                post = [id,
                        created_at,
                        url,
                        likes,
                        retweets,
                        replies,
                        views,
                        text]
                # if(post not in posts):
                posts.append(post)
                if(browser.execute_script(f"return window.scrollY") == browser.execute_script(f"return document.body.scrollHeight")):
                    time.sleep(4)
                browser.execute_script(f"window.scrollBy(0, 500)")
        except Exception as e:
            time.sleep(0.25)
            browser.execute_script(f"window.scrollBy(0, 200)")
            repetitive_scroll +=1
            pass
        position = browser.execute_script(f"return window.scrollY")
        if( position == last_position ):
            print(position, last_position)
            continue_scrolling= False
            break
    # 'usernames','hashtags','emojis','emoji_text'
    df = pd.DataFrame(data = posts, columns = ['id','created_at','url','likes','retweets','replies','views','text'])
    df = df.drop_duplicates(subset=['id'], keep='last').sort_values('created_at', ascending=False).reset_index(drop=True)
    return df

# %%
# configure browser
def configure_browser(headless=True, fullscreen=False, random_agent=False, w = 782, h=871, x=761, y=0):
    
    print("Set Up Browser Configurations")
    options = webdriver.ChromeOptions()
    if(headless == True): # browser without GUI interface
        options.add_argument('--headless')
    if(random_agent==True): # Set Random Agent
        pass
        options.add_argument(f"user-agent={UserAgent().random}")
    else: # Set User Agent
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"+ "AppleWebKit/537.36 (KHTML, like Gecko)"+ "Chrome/114.0.0.0 Safari/537.36")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--ignore-certificate-errors')
    options.add_extension(f'./drivers/NoBufferCRX0.4.4.crx') # Stop video's from auto playing
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-popup-blocking')
    # remove password saving and geolocation
    prefs = {"profile.default_content_setting_values.geolocation" :2,
             "credentials_enable_service": False,
             "profile.password_manager_enabled": False}
    options.add_experimental_option("prefs", prefs)
    # options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # hard coded chromedriver
    browser = webdriver.Chrome(service=Service(executable_path=f"./drivers/chromedriver.exe"), options=options, keep_alive=False)
    # using chromedriver manager
    # browser = webdriver.Chrome(service=ChromeDriverManager(chrome_type=ChromeType.GOOGLE, path = r"./drivers").install(), options=options, keep_alive=False)
    agent = browser.execute_script("return navigator.userAgent")
    if(fullscreen == True):
        browser.fullscreen_window()
    else:
        browser.set_window_size(width=w, height=h) #{'width': 782, 'height': 871}
        browser.set_window_position(x=x, y=y) #{'x': 761, 'y': 0}
    window_dim = browser.get_window_rect()
    return browser, window_dim, agent

def load_users():
    
    # Load Twitter Usernames   
    with open(os.path.normpath(os.getcwd() + '/user_input/twitter_users.xlsx'), 'rb') as f:
        user_df = pd.read_excel(f, sheet_name='user_names')
        user_df = user_df.where(pd.notnull(user_df), '')
        f.close()
    twitter_groups = list(user_df.columns)
    return user_df, twitter_groups

def task(user):
    
    # Open Browser
    browser,window_dim,agent = configure_browser(headless = True, 
                                                fullscreen=False, 
                                                random_agent=False, 
                                                w = 782, h=871, 
                                                x=761, y=0)
    
    # load twitter credentials
    credentials = pd.read_csv(f'./credentials/twitter.csv', header=0)
    email,password,phone = credentials.email[0],credentials.password[0],credentials.phone[0]
    
    # Login to Twitter
    twitter_login(browser= browser, 
                email= email, 
                password= password, 
                phone = phone)
    # extract twitter
    df = extract_twitter(browser, user)
    print(df.head())
    
def multi_process():
    
    # 16 cpu's minus 1 for core system processes 
    cpu_count = multiprocessing.cpu_count() - 1
    p = multiprocessing.Pool(cpu_count)
    
    user_df, twitter_groups = load_users()
    for group in user_df:
        print(f"\n{group}:\n")
        users = list(user_df[group][user_df[group]!= ''])
        folder = f'./data/extracted/raw/twitter/{group}'
        for user in users:
            print(user)
            file = f'{folder}{os.sep}{user}.parquet'
            p.apply_async(task, args={user})
    p.close()
    p.join()

if __name__ == '__main__':

    multi_process()
#%%