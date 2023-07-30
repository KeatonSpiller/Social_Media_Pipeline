
# *** Import Libraries ***
# 1. Standard library imports. ( https://docs.python.org/3/library/index.html )
import os 
import time
import multiprocessing
import advertools
# 2. Related third party imports.
import itertools
import pandas as pd 
import numpy as np 
import glob
import re 
import json
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common import action_chains
from selenium.common import exceptions
from fake_useragent import UserAgent
# import browser_cookie3

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
        
def get_credentials():

    credentials = pd.read_csv(os.path.abspath('./credentials/twitter.csv'), header=0, sep=',')
    email, password, phone = credentials.email[0], credentials.password[0], credentials.phone[0]
    return email, password, phone

# configure browser
def configure_browser(headless=True, fullscreen=False, agent='standard', w = 782, h=871, x=761, y=0, debug=False):
    options = webdriver.ChromeOptions()
    # 'none', 'eager', 'normal'
    options.page_load_strategy = 'eager'
    if debug:
        print("Setting Up Web Browser Configuration")
    # Hide Chrome Tab
    if headless: 
        options.add_argument('--headless')
    if fullscreen:
        options.add_argument('--start-maximized')
    # Disable Below
    options.add_argument('--no-sandbox') # Running as root without --no-sandbox is not supported.
    options.add_argument("--disable-dev-shm-usage") # No such file or directory Creating shared memory in /dev/shm/.org.chromium.Chromium.JwBSnH
    # Videos
    options.add_extension(f'./drivers/NoBuffer.crx') # Stop video's from auto playing
    # Images
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--disable-gpu=true')
    # Notifications
    options.add_argument('--ignore-certificate-errors')
    # options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-crash-reporter")
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-popup-blocking')
    # Detection of Bots Automation
    options.add_argument('--disable-blink-features=AutomationControlled')
    # options.add_argument("--disable-infobars")
    options.add_argument("--kiosk") # printer mode
    prefs = {"profile.default_content_setting_values.geolocation" :2, # remove Ask location
             "profile.password_manager_enabled": False, # remove Ask password saving
             "profile.managed_default_content_settings.images": 2, # remove images
             "credentials_enable_service": False, # remove ask password
             "useAutomationExtension": False
             } 
    options.add_experimental_option("prefs", prefs)
    # options.add_experimental_option('useAutomationExtension', False)
    # remove DevTools listening
    options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation']) 
    # remove "Chrome is  being controlled by automated test software"
    # options.add_experimental_option("excludeSwitches", ['enable-automation'])
    # Configure the Type of agent
    if(agent=='random'): # Set Random Agent
        options.add_argument(f"user-agent={UserAgent().random}")
    if(agent=='standard'): # Set Desktop Google Chrome
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"+ "AppleWebKit/537.36 (KHTML, like Gecko)"+ "Chrome/114.0.0.0 Safari/537.36")
    if(agent=='googlebot'): # Set Google Bot ( Disable login verification )
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) + AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1; +http://www.google.com/bot.html) Chrome/114.0.0.0 Safari/537.36")
    if(agent=='okhttp'):
        options.add_argument("user-agent=okhttp/4.10.0")
        # options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) + AppleWebKit/537.36 (KHTML, like Gecko; compatible; okhttp/4.10.0;) Chrome/114.0.0.0 Safari/537.36")
    if(agent == 'yandex'):
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 YaBrowser/23.7.0.2564 Yowser/2.5 Safari/537.36")
    if(agent == 'bingbot'):
        options.add_argument("user-agent=Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm) Chrome/")
        
    # local chromedriver
    browser = webdriver.Chrome(service=Service(executable_path=f"./drivers/chromedriver.exe"), options=options, keep_alive=False)
    # chromedriver manager
    # browser = webdriver.Chrome(service=ChromeDriverManager(chrome_type=ChromeType.GOOGLE, path = r"./drivers").install(), options=options, keep_alive=False)
    if(fullscreen == True):
        browser.maximize_window()
    else:
        # half page
        browser.set_window_size(width=w, height=h) #{'width': 782, 'height': 871}
        browser.set_window_position(x=x, y=y) #{'x': 761, 'y': 0}
    # Zoom out to find more on page ( Note Currently Not displaying )
    browser.execute_script("document.body.style.zoom='zoom 50%'")
    browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    return browser

def load_users():
    
    # Load Twitter Usernames   
    with open(os.path.normpath(os.getcwd() + '/user_input/twitter_users.xlsx'), 'rb') as f:
        user_df = pd.read_excel(f, sheet_name='user_names')
        user_df = user_df.where(pd.notnull(user_df), '')
        f.close()
    return user_df

# def twitter_login_bot(browser, login_url, email, password, phone, agent, debug):
    
#     browser.get(login_url)
#     if debug:
#         print("Logging Into Twitter")
#     wait = WebDriverWait(browser, 30)
#     try:
#         if debug:
#             print("Email Login")
#         username_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@type='email']")))
#         username_input.send_keys(str(email))
#     except Exception as e:
#         if debug:
#             print("failed to login With Email")
#         pass
#     try:
#         if debug:
#             print("Password Login")
#         password_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[@type='password']")))
#         password_input.send_keys(str(password))
#         password_input.send_keys(Keys.RETURN)
#         time.sleep(4)
#     except Exception as e:
#         if debug:
#             print("failed to login With Password")
#         pass
#     try:
#         login_button = wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@role = 'button']")))
#         login_button.send_keys(str(password))
#         if debug:
#             print("Done Logging In")
#     except Exception as e:
#         if debug:
#             print("failed to click login button")
#         pass
        
        
def twitter_login(browser, login_url, email, password, phone, agent, debug):
    
    browser.get(login_url)
    if debug:
        print("Logging Into Twitter")
    wait = WebDriverWait(browser, 30)
    try:
        if debug:
            print("Email Login")
        username_input = wait.until(EC.visibility_of_element_located((By.NAME, "text")))
        username_input.send_keys(str(email))
        wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@role='button'][contains(.,'Next')]"))).click()
    except Exception as e:
        if debug:
            print("failed to login With Email")
        pass
    try:
        if debug:
            print("Phone Login")
        phone_input = WebDriverWait(browser, 4).until(EC.visibility_of_element_located((By.NAME, "text")))
        phone_input.send_keys(str(phone))
        phone_input.send_keys(Keys.RETURN)
    except exceptions.TimeoutException :
        if debug:
            print("No phone Number Required")
        pass
    try:
        if debug:
            print("Password Login")
        password_input = wait.until(EC.visibility_of_element_located((By.NAME, "password")))
        password_input.send_keys(str(password))
        password_input.send_keys(Keys.RETURN)
        time.sleep(4)
    except Exception as e:
        if debug:
            print("failed to login With Password")
        pass
    if debug:
        print("Done Logging In")

def extend_df_text(df):
    
        # Extract Variables From Text
        extract = extract_from_text(df.text)
        websites = extract.websites()
        hashtags = extract.hashtags()
        usernames = extract.usernames()
        emojis,emoji_text = extract_from_text(df.emojis).emojis()
        df = df.assign(hashtags = hashtags,
                    websites = websites,
                    usernames = usernames,
                    emojis = emojis,
                    emoji_text = emoji_text)
        column_to_move = df.pop("text")
        df.insert(2, "text", column_to_move)
        return df
    
def extract_elements_text(elements):
                    
    text,emoji_list = [],[]
    for txt in elements:
        text.append(txt.text.encode(encoding='utf-8').decode(encoding='utf-8'))
        emoji_element = txt.find_elements(By.XPATH, "./child::*[self::img]")
        emojis = ''
        # Check for all emoji children of text
        for e in emoji_element:
            temp = e.get_attribute('alt')
            if(temp is not None):
                emojis = emojis + temp
        emoji_list.append(emojis)
        
    return text, emoji_list

def extract_elements_metrics(elements):
                    
    replies_list,retweets_list,likes_list,views_list = [],[],[],[]
    for block in elements:
        
        replies,likes,views,retweets = 0,0,0,0
        metric = block.get_attribute('aria-label')
        metric_num = re.findall('[\d]+', metric)
        metric_string = re.findall('[a-zA-Z]+', metric)
        
        for s,num in zip(metric_string, metric_num):
            if(s == 'reply' or s =='replies'):
                replies= num
            if(s == 'Retweets' or s =='Retweet'):
                retweets = num
            if(s == 'like' or s =='likes'):
                likes = num
            if(s == 'views' or s =='view'):
                views = num
        replies_list.append(replies)
        retweets_list.append(retweets)
        likes_list.append(likes)
        views_list.append(views)
        
    return replies_list,retweets_list,likes_list,views_list

def extract_elements_links_id(elements):
    urls, ids = [],[]
    # remove duplicate external links
    for link in elements:
        href = link.get_attribute('href')
        last_child = str(href.split('/')[-1])
        if(len(last_child) >= len("1681508122635169792")):
            urls.append(href)
            ids.append(int(last_child))
    return ids, urls      
           
def extract_twitter_user(browser, user='CNN', scroll_by= 800, howmany_loops = np.Infinity, howfar='1970-01-01', sleep= 4, debug=False):
    
        metric_var = ['likes', 'views', 'Retweets', 'replies', 'like', 'view', 'Retweet', 'reply']
        posts = []
        metric_conditional = ''
        continue_scrolling = True
        howfar_reached = False
        repetitive_scroll = 0
        scroll_count = 1
        
        browser.get(f'https://www.twitter.com/{user}')
        try:
            user_name = WebDriverWait(browser, 20).until(EC.visibility_of_element_located((By.XPATH, '//div[@data-testid="UserName"]'))).get_attribute("innerText")
            if debug:
                print(f'\nExtracting\n')
        except Exception as e:
            if debug:
                print(f"Incorrect username {user}")
            pass
        browser.execute_script(f"window.scrollTo(0, 0)") # 3 variations (scrollBy, scrolTo, scroll)
        
        # Remove Login Blue Strip "Login" or "Don’t miss what’s happening"
        blue_strip_top = browser.find_elements(By.XPATH, f"//*[@data-testid = 'TopNavBar']")
        [browser.execute_script("arguments[0].remove()",child) for child in blue_strip_top]
        blue_strip_bottom = browser.find_elements(By.XPATH, f"//*[@data-testid = 'BottomBar']")
        [browser.execute_script("arguments[0].remove()",child) for child in blue_strip_bottom]
        
        for i in range(0, len(metric_var)):
            for j in range(i+1, len(metric_var)-1):
                if ( i != j):
                    var1= metric_var[i]
                    var2= metric_var[j]
                    if( var1[0:2] != var2[0:2] ):
                        metric_conditional += f"(contains(@aria-label, '{var1}') and contains(@aria-label, '{var2}')) or "
        metric_conditional_string = f"//*[{metric_conditional}"[0:-4] +"]"
        
        while(continue_scrolling == True and repetitive_scroll <= 2 and howfar_reached == False and howmany_loops != scroll_count):
            scroll_count += 1
            last_position = browser.execute_script(f"return window.scrollY")
            repetitive_scroll = 0
            try:
                # How much to sleep to avoid breaking the program
                time.sleep(sleep)
                # XPATH HELP https://devhints.io/xpath
                page_text = WebDriverWait(browser, 0.5).until(EC.presence_of_all_elements_located((By.XPATH, '//div[@data-testid="tweetText"][span]')))
                page_timestamps = WebDriverWait(browser, 0.5).until(EC.presence_of_all_elements_located((By.XPATH, "//time")))
                page_links = WebDriverWait(browser, 0.5).until(EC.presence_of_all_elements_located((By.XPATH, f"//a[contains(@href, '/{user}/status/')]")))
                page_metrics = WebDriverWait(browser, 0.5).until(EC.presence_of_all_elements_located((By.XPATH, metric_conditional_string)))
     
                # Iterate Individual element clusters (Note zip() gives duplicates if trying to iterate simultaneously on DOM)
                ids, urls = extract_elements_links_id(page_links)
                text, emojis = extract_elements_text(page_text)  
                replies, retweets, likes, views = extract_elements_metrics(page_metrics)
                created_at = [pd.to_datetime(ts.get_attribute('datetime')) for ts in page_timestamps]
                post_block = list(zip(ids, created_at, urls, likes, retweets, replies, views, emojis, text))
                posts.extend(post_block)
                # If want tweets for a specific end Range
                if(any(created_at <= pd.to_datetime(howfar)) ):
                    howfar_reached = True
                    if debug:
                        print("reached {howfar}\n")
                    break
                # Give Time For the Page to Refresh Elements
                if(browser.execute_script(f"return window.scrollY") == browser.execute_script(f"return document.body.scrollHeight")):
                    time.sleep(sleep)
                browser.execute_script(f"window.scrollBy(0, {scroll_by})") # how far to scroll to find elements on screen
        
            except Exception as e: # If there are not elements present on screen how far to scroll
                time.sleep(sleep)
                browser.execute_script(f"window.scrollBy(0, {scroll_by})")
                repetitive_scroll +=1
                pass
            position = browser.execute_script(f"return window.scrollY")
            if( position == last_position ): # If we reached the end of possible scrolling
                if debug:
                    print(f"Reached End of document: current position: {position}, last position: {last_position}")
                continue_scrolling= False
                break
        df = pd.DataFrame(data = posts, columns = ['id','created_at','url','likes','retweets','replies','views','emojis','text'])
        df = df.drop_duplicates(subset=['id'], keep='last').sort_values('created_at', ascending=False).reset_index(drop=True)
        return df
    
def twitter_web_crawl(arg):
    
    # unpack tuple
    (user,login_url,cookies,agent,group,folder,scroll_loops,scroll_by,headless,full_screen,howfar,sleep,debug) = arg
    # Open Browser
    browser = configure_browser(headless = headless, 
                                fullscreen=full_screen,
                                agent = agent,  
                                w = 782, h=871, 
                                x=761, y=0,
                                debug=debug)
    browser.get(login_url)
    add_all_cookies(browser=browser,
                    cookies=cookies)
    # Extract twitter
    df = extract_twitter_user(browser=browser, 
                              user=user,
                              scroll_by = scroll_by,
                              howmany_loops=scroll_loops,
                              howfar = howfar,
                              sleep=sleep,
                              debug=debug)
    print(user, len(df))
    browser.close()
    # Convert Column Data Types to this dictionary
    df_dtypes = {'id':         'int64',
                'created_at': 'datetime64[ns, UTC]',
                'text':       'object',
                'url':        'object',
                'likes':      'int64',
                'retweets':   'int64',
                'replies':    'int64',
                'views':      'int64',
                'emojis':     'object',
                'hashtags':   'object',
                'websites':   'object',
                'usernames':  'object',
                'emoji_text': 'object'}
    # Extend dataframe
    df = extend_df_text(df).astype(df_dtypes)
    file = f'{folder}{os.sep}{group}{os.sep}{user}.parquet'
    output = (df, folder, group, user, file, df_dtypes)
    merge_and_export(output)

# Export and Merge
def merge_and_export(output):
    (df, folder, group, user, file, df_dtypes) = output
    if(os.path.exists(file)):# combine with previous file
        df_history = pd.read_parquet(path=file).reset_index(drop=True).astype(df_dtypes)
        if(len(df_history) > 0 ):
            df_merge = pd.concat([df_history, df], join="outer", ignore_index=True).drop_duplicates(subset=['id'], keep='last').reset_index(drop=True)
            df_merge.to_parquet(path=file,index=False)
            downloaded = len(df_merge) - len(df_history)
            print(f'{group}: {user} -> {downloaded} new tweets downloaded, {len(df_merge)} tweets total', end='\n') 
        else: # if file was deleted
            df.to_parquet(path=file,index=False)            
    else: # first time downloading
        df.to_parquet(path=file,index=False)
        print(f'{group}: {user} -> {len(df)} new tweets downloaded', end='\n')
            
def parallel_extract_twitter(user_df, folder=f'./data', login_url='https://twitter.com/i/flow/login', agent='standard', headless = True, full_screen=False, scroll_loops = np.Infinity, howfar = '2000-01-01', scroll_by = 800, sleep=4, cookies=None, debug=False):
    """_summary_
    Args:  
    
    user_df (_type_): pandas.DataFrame()
    
    | US_journals | international_news  |
    |----------   |---------------------|
    |0      WSJ   |   ftworldnews       |
    |1      CNN   |   guardian          |
    |2		      |   FinancialTimes    |
    """
    cpu_count = multiprocessing.cpu_count() - 1 # max(cpu) minus 1 for core system processes 
    p = multiprocessing.Pool(cpu_count) # p.apply_async, p.apply, p.map, p.starmap_async -> (# of arguments and returns)?
    print(f"\nTwitter Data Extraction")
    if(not(os.path.exists(folder))):# location to store Raw twitter downloads
        os.makedirs(folder)
    # find cookies of browser at a specifc webpage to pass into other browsers
    if cookies == None:
        cookies = get_cookies_session(url= login_url, 
                                    agent= agent,
                                    debug=debug, 
                                    headless=headless)
    for group in user_df.columns:
        if debug:
            print(f"\n***{group}***\n")
        group_folder = f'{folder}{os.sep}{group}'
        if(not(os.path.exists(group_folder))): # Splitting Twitter downloads into groups per user
            os.makedirs(group_folder)
        users = list(user_df[group][user_df[group]!= ''])
        for user in users:
            if debug:
                print(f"{user}", end=" ")
            input = (user,login_url,cookies,agent,group,folder,scroll_loops,scroll_by,headless,full_screen,howfar,sleep,debug)
            
            if debug: # Synchronous debugging (try catch errors doesn't always work in async)
                p.map(func=twitter_web_crawl, iterable=(input,))
            else:
                p.apply_async(func=twitter_web_crawl, args=(input,))
    p.close()
    p.join()
    # log out of all sessions ( Deprecated: Login No Longer Required ) 
    # if( agent != 'bot'):
    #     log_out_sessions((headless,full_screen,agent,debug))
    return
    
def read_write_cookies(url,browser):
    
    cookies_folder = f'./credentials/cookies'
    # reading and loading cookies from a json file
    if not os.path.exists(cookies_folder):# If first time saving cookies
        os.mkdir(cookies_folder)
        cookies = browser.get_cookies()
        # browser.close()
        with open(cookies_folder+'/cookies', 'w') as fout:
            json.dump(cookies, fout)
    else: # If cookies are saved
        with open(cookies_folder+'/cookies', 'r') as input:
            cookies = json.load(input)
    return cookies
            
def get_cookies_session(url, agent, debug, headless):

    browser = configure_browser(headless = headless, 
                                fullscreen= False, 
                                agent= agent,
                                w = 782, h=871, 
                                x=761, y=0, 
                                debug=debug)
    email, password, phone = get_credentials()
    if agent != 'googlebot':
        twitter_login(browser= browser,
                    login_url = url,
                    email= email,
                    password= password,
                    phone = phone,
                    agent = agent,
                    debug = debug)
    browser.get(url)
    cookies = browser.get_cookies()
    # browser.close()
    
    return cookies

def add_all_cookies(browser, cookies):
    
    # df = pd.DataFrame(cookies)
    # browser.delete_all_cookies()
    for cookie in cookies:
        try:
            browser.add_cookie(cookie)
        except Exception:
            print("ERROR", cookie)
    return browser
    
def log_out_sessions(arg):
    (headless,full_screen,agent,debug) = arg
    session_link = f'https://twitter.com/settings/sessions'
    email, password, phone = get_credentials()
    # Open Browser
    browser = configure_browser(headless = headless, 
                                fullscreen=full_screen, 
                                agent= agent,
                                w = 782, h=871, 
                                x=761, y=0, debug=debug)
    # Login to Twitter
    twitter_login(browser= browser, 
                    email= email, 
                    password= password, 
                    phone = phone,
                    agent = agent,
                    debug = debug)
    # Open Session_link
    browser.get(session_link)
    time.sleep(4)
    # Click Logout and Confirm
    browser.find_elements(By.XPATH, ("//*[@role = 'button']/child::*/child::*[self::span][contains(text(), 'Log out of all other sessions')]"))[0].click()
    browser.find_elements(By.XPATH, ("//*[@data-testid = 'confirmationSheetConfirm']"))[0].click()
    
class extract_from_text:
    
    def __init__(self, s):
        self.s = s
        self.username = r'@[\w]+'
        self.hashtag = r'#\s+\w+|#\w+'
        self.website = r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})"
        
    def hashtags(self):
        s = self.s
        hashtags = s.str.findall(self.hashtag, flags=re.IGNORECASE).str.join(" ").str.strip()
        return hashtags
    
    def usernames(self):
        s = self.s
        usernames = s.str.findall(self.username, flags=re.IGNORECASE).str.join(" ").str.strip()
        return usernames
    
    def websites(self):
        s = self.s
        links = s.str.findall(self.website, flags=re.IGNORECASE).str.join(" ").str.strip()
        return links
    
    def emojis(self):
        s = self.s
        
        if(s.empty):
            # There are no emoji's
            emojis = s
            emoji_text = s
        else:
            emoji_extraction = advertools.extract_emoji(s)
            emojis = pd.Series(emoji_extraction['emoji']).str.join(" ")
            emoji_text = pd.Series(emoji_extraction['emoji_text']).str.join(" ")
        
        return emojis, emoji_text