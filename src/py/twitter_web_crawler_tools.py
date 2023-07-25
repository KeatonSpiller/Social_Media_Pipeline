
# *** Import Libraries ***
# 1. Standard library imports. ( https://docs.python.org/3/library/index.html )
import os 
import time
import multiprocessing
import advertools
# 2. Related third party imports.
import pandas as pd 
import numpy as np 
import glob
import re 
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common import exceptions
from fake_useragent import UserAgent

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

# %%
# configure browser
def configure_browser(headless=True, fullscreen=False, random_agent=False, w = 782, h=871, x=761, y=0):
    
    print("Setting Up Web Browser Configuration")
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
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-crash-reporter")
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
    # remove DevTools listening
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # hard coded chromedriver
    browser = webdriver.Chrome(service=Service(executable_path=f"./drivers/chromedriver.exe"), options=options, keep_alive=False)
    # using chromedriver manager
    # browser = webdriver.Chrome(service=ChromeDriverManager(chrome_type=ChromeType.GOOGLE, path = r"./drivers").install(), options=options, keep_alive=False)
    # agent = browser.execute_script("return navigator.userAgent")
    if(fullscreen == True):
        browser.maximize_window()
    else:
        browser.set_window_size(width=w, height=h) #{'width': 782, 'height': 871}
        browser.set_window_position(x=x, y=y) #{'x': 761, 'y': 0}
    # window_dim = browser.get_window_rect()
    # return broswer, window_dim, agent
    return browser

def load_users():
    
    # Load Twitter Usernames   
    with open(os.path.normpath(os.getcwd() + '/user_input/twitter_users.xlsx'), 'rb') as f:
        user_df = pd.read_excel(f, sheet_name='user_names')
        user_df = user_df.where(pd.notnull(user_df), '')
        f.close()
    return user_df

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

# %%            
def extract_twitter_user(browser, user='CNN', howmany_loops = np.Infinity, howfar='2000-01-01' ):
    
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
            print(f'\nExtracting\n')
        except Exception as e:
            print(f"Incorrect username {user}")
            pass
        browser.execute_script(f"window.scrollTo(0, 0)") # 3 variations (scrollBy, scrolTo, scroll)
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
            try:
                repetitive_scroll = 0
                # How much to sleep to avoid breaking the program
                time.sleep(2)
                # https://devhints.io/xpath
                page_text = WebDriverWait(browser, 0.5).until(EC.presence_of_all_elements_located((By.XPATH, '//div[@data-testid="tweetText"]')))
                page_timestamps = WebDriverWait(browser, 0.5).until(EC.presence_of_all_elements_located((By.XPATH, "//time")))
                page_links = WebDriverWait(browser, 0.5).until(EC.presence_of_all_elements_located((By.XPATH, f"//a[contains(@href, '/{user}/status/')]")))
                page_metrics = WebDriverWait(browser, 0.5).until(EC.presence_of_all_elements_located((By.XPATH, metric_conditional_string)))
                links,ids = [],[]
                
                # remove duplicate external links
                for link in page_links:
                    href = link.get_attribute('href')
                    last_child = str(href.split('/')[-1])
                    if(len(last_child) >= len("1681508122635169792")):
                        links.append(href)
                        ids.append(int(last_child))
                ## page_source = bs(browser.page_source,'lxml')
                
                def check_dimensions(variables):
                    dimensions = list(map(len,variables))
                    if len(set(dimensions)) == 1:
                        return dimensions
                    else:
                        for i in range(1, dimensions):
                            if( dimensions[0] - dimensions[i] > 1):
                                print(f"Error in Dimensions Downloaded\n{dimensions}")
                                break
                        
                # variables = (page_text,page_metrics,page_timestamps,ids,links)
                # dimensions = check_dimensions(variables)
                # print(dimensions)
                
                # initalize if metrics are empty
                replies,likes,views,retweets = 0,0,0,0
                post = []
                for txt,metrics,ts,link,id in zip(page_text,page_metrics,page_timestamps,links,ids):
                    
                    text = txt.text.encode(encoding='utf-8').decode(encoding='utf-8')
                    metric = metrics.get_attribute('aria-label')
                    metric_num = re.findall('[\d]+', metric)
                    metric_string = re.findall('[a-zA-Z]+', metric)
                    created_at = pd.to_datetime(ts.get_attribute('datetime'))
                    url = link
                    emojis = ""
                    # Check for all emoji children of text
                    emoji_element = txt.find_elements(By.XPATH, "./child::*[self::img]")
                    for e in emoji_element:
                        temp = e.get_attribute('alt')
                        if(temp is not None):
                            emojis = emojis + temp
                        
                    for s,num in zip(metric_string, metric_num):
                        if(s == 'reply' or s =='replies'):
                            replies= num
                        if(s == 'Retweets' or s =='Retweet'):
                            retweets = num
                        if(s == 'like' or s =='likes'):
                            likes = num
                        if(s == 'views' or s =='view'):
                            views = num
                    post = [id, created_at, url,
                            likes, retweets, replies,
                            views, emojis, text]
                    if post not in posts:
                        posts.append(post)
                    
                    if(post[1] <= pd.to_datetime(howfar) ):
                        howfar_reached = True
                        print("reached {howfar}\n")
                        break
                    
                    if(browser.execute_script(f"return window.scrollY") == browser.execute_script(f"return document.body.scrollHeight")):
                        time.sleep(4)
                    browser.execute_script(f"window.scrollBy(0, 800)") # how far to scroll to find elements on screen
        
            except Exception as e: # If there are not elements present on screen how far to scroll
                time.sleep(0.25)
                browser.execute_script(f"window.scrollBy(0, 400)")
                repetitive_scroll +=1
                pass
            position = browser.execute_script(f"return window.scrollY")
            if( position == last_position ): # If we reached the end of scrolling access
                print(position, last_position)
                continue_scrolling= False
                break
        df = pd.DataFrame(data = posts, columns = ['id','created_at','url','likes','retweets','replies','views','emojis','text'])
        df = df.drop_duplicates(subset=['id'], keep='last').sort_values('created_at', ascending=False).reset_index(drop=True)
        browser.close()
        return df
    
def twitter_web_crawl(arg):
    
    # unpack tuple
    user = arg[0]
    group = arg[1]
    folder = arg[2]
    email, password, phone = get_credentials()
    # Open Browser
    browser = configure_browser(headless = False, 
                                fullscreen=False, 
                                random_agent=False, 
                                w = 782, h=871, 
                                x=761, y=0)
    # Login to Twitter
    twitter_login(browser= browser, 
                  email= email, 
                  password= password, 
                  phone = phone)
    # Extract twitter
    df = extract_twitter_user(browser=browser, 
                              user=user,
                              howmany_loops=30).copy()
    # Convert Column Data Types to this dictionary
    df_dtypes = {'id':         'int64',
                'created_at': 'datetime64[ns, UTC]',
                'text':       'object',
                'url':        'object',
                'likes':      'int64',
                'retweets':   'int64',
                'replies':    'object',
                'views':      'object',
                'emojis':     'object',
                'hashtags':   'object',
                'websites':   'object',
                'usernames':  'object',
                'emoji_text': 'object'}
    # Extend dataframe
    df = extend_df_text(df).astype(df_dtypes)
    # Export and Merge
    # *********************************************** NOT WORKING ***********************************************
    file = f'{folder}{os.sep}{group}{os.sep}{user}.parquet'
    if(os.path.exists(file)):
        df_history = pd.read_parquet(path=file).reset_index(drop=True).astype(df_dtypes)
        if(len(df_history) > 0 ):
            df_merge = pd.concat([df_history, df], how="outer", ignore_index=True).drop_duplicates(subset=['id'], keep='last').reset_index(drop=True)
            df_merge.to_parquet(path=file,index=False)
            print(df_merge.size)
        else:
            df.to_parquet(path=file,index=False)            
        # print(f"{group}-{user}: {df_merge.size[0] - df_history.size[0]} extracted, {df_merge.size[0]} total")
    else:
        df.to_parquet(path=file,index=False)
        # print(f"{group}-{user}:  {df_merge.size[0]} extracted for Initial download")
        print(df_merge.size)
    # merge_and_export(df.copy(), user, group, file)  
       
# def merge_and_export(df, user, group, file):

            
def parallel_extract_twitter(user_df, folder=f'./data'):
    """_summary_
    Args:  
    
    user_df (_type_): pandas.DataFrame()
    
    |US_journals  | international_news  |
    |----------   |---------------------|
    |0    WSJ	  |   ftworldnews       |
    |1    CNN	  |   guardian          |
    |2		      |   FinancialTimes    |
    """
    cpu_count = multiprocessing.cpu_count() - 1 # max(cpu) minus 1 for core system processes 
    p = multiprocessing.Pool(cpu_count)
    
    # folder to store Raw twitter downloads
    if(not(os.path.exists(folder))):
        os.makedirs(folder)
    for group in user_df.columns:
        print(f"\n***{group}***")
        group_folder = f'{folder}{os.sep}{group}'
        if(not(os.path.exists(group_folder))):
            os.makedirs(group_folder)
        users = list(user_df[group][user_df[group]!= ''])
        for user in users:
            print(f"\n{user}:\n")
            tup = (user,group,folder)
            p.apply_async(twitter_web_crawl, args=(tup,))
    p.close()
    p.join()
    
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