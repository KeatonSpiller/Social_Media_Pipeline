# %% [markdown]
## Parse Data
#       Summary Overview
#   - Using Tweepy API to parse data from Twitter API
#   - 200 chunks of tweets for 3200 tweets for each User in user_input
#   - strips emoji's, hashtags, usernames, and website links into seperate columns

# %% [markdown]
## Import Libraries
import os, pandas as pd, tweepy, numpy as np, tweepy, glob, re, advertools

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
  
# %% [markdown]
## Load Custom Functions
from extract_twitter_tools import user_download, twitter_authentication, merge_tweets

# %% [markdown]
# # Twitter API Credentials
# Read in keys from a csv file
def twitter_authentication_v2(autentication_path):
    """_summary_
    Read in twitter api credentials stored on csv file under user_input
    _why_
    Args:
        autentication_path (_type_): _description_
    """
    
    readin_authentication = pd.read_csv(autentication_path, header=0, sep=',')
    consumer_key = readin_authentication['consumer_key'][0]
    consumer_secret = readin_authentication['consumer_secret'][0]
    access_token = readin_authentication['access_token'][0]
    access_token_secret = readin_authentication['access_token_secret'][0]
    bearer_token = readin_authentication['beaker_token'][0]
    client_id = readin_authentication['client_id'][0]
    client_secret = readin_authentication['client_secret'][0]

    # connect to twitter application 
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    # auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit = True)
    
    # api = tweepy.Client(bearer_token=bearer_token,
    #                     wait_on_rate_limit = True)
    return api

authentication_path = os.path.abspath('./credentials/twitter.csv')
# api = twitter_authentication(authentication_path)
api = twitter_authentication_v2(authentication_path)

# print(api.update_status('453'))

# api.get_user(screen_name='CNN')
# tweet = api.user_timeline(screen_name='CNN', 
#                         count=200,
#                         include_rts = False,
#                         trim_user = False,
#                         tweet_mode = 'extended')

# tweets = api.get_users_tweets(id = 'CNN',
#                                 max_results =100)
# tweets = api.user_timeline(screen_name='CNN', 
#                                 count=200,
#                                 include_rts = False,
#                                 trim_user = False,
#                                 tweet_mode = 'extended')

# %%
# # Load Twitter Usernames   
# * Accounts may be privated or removed and change ability to download
# * No two users can have the same id
with open(os.path.normpath(os.getcwd() + '/user_input/twitter_users.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
twitter_groups = list(user_df.columns)

# def dataframe_astypes():
#     """_summary_
    
#     cleanly access dataframe conversions
    
#     Returns:
#         dictionary: column names and pandas dataframe conversions
#     """
#     return { 'id': 'int64',
#             'created_at': 'datetime64[ns, UTC]',
#             'url': 'object',
#             'favorite_count': 'int64',
#             'retweet_count': 'int64',
#             'hashtags':'object',
#             'emojis': 'object',
#             'emoji_text':'object',
#             'usernames': 'object',
#             'links': 'object',
#             'text': 'object'}

# def user_download(api, user_list, group, folder, display='full'):
#     """_summary_
    
#     Download users within Excel list of usernames and save in a parquet under data
#     _why_
    
#     runs user_download_helper for each user to download tweets
#     Args:
#         Args input:
#         api (tweepy class): Handles parsing Twitter API
#         userID (list): twitter usernames
#         group (string): label of users in list
#     Args output:
#         parquet file of users for each group
#     """
#     # Download every User from Group of User's
#     for userID in user_list:
#         try:
#             file = f'{folder}{os.sep}{userID}.parquet'
#             if(display == 'minimal' or display == 'full'):
#                 print(userID, end=' ')
#             user_download_helper(api=api, 
#                                  userID=userID, 
#                                  group=group, 
#                                  file=file, 
#                                  folder=folder, 
#                                  display=display)
#         except Exception as e:
#             print(f"\n{userID}", end = ' ')
#             print(f"exception: {str(e)}",
#                   f"Scenarios could include",
#                   f"- Mispelled User ID",
#                   f"- Account Removed",
#                   f"- Privated account",
#                   f"*****", sep='\n', end='\n\n')

# def user_download_helper(api, userID, group, file, folder, display):
#     """_summary_
    
#     Tweepy API download limit of 200 per chunk, Twitter API limit of 3200
#     Looping over individual user using usertimeline to append maximum tweets
#     limits on time scraping data, will occasionally wait ~ 15 then continue
#     removed usernames, punctuation, website links and special charactersfrom text
#     converted time zone from UTC to EST
#     _why_
    
#     Strip particular user data from Twitter
#     Args input:
#         api (tweepy class): Handles parsing Twitter API
#         userID (list of strings): twitter usernames
#         group (string): label of users in list
#         display (string): 'full' -> prints user and size of tweets added or [no recent tweets]
#                           'minimal' -> prints user
#                            [any other keystroke] -> prints only the group downloading
#                            Default: 'full'
#     Args output:
#         parquet file in data/users/{group}/{username}.parquet
#     """
#     first_time_dowloading = False
#     # Check if there exists previous history of user and group
#     if os.path.exists(file):
#         try:
#             df_history = pd.read_parquet(file).reset_index(drop=True)
#             oldest_id = df_history.id.min()
#             since_id = df_history.id.max()
#             df_history.created_at = pd.to_datetime(df_history.created_at)
#             # df_history.dtypes({"created_at":"datetime64[ns, UTC]"})
            
#         except Exception:
#             print("folder created without previous file")
#     else:
#         oldest_id = None
#     all_tweets = []
    
#     # If first time running
#     #################################################
#     if(oldest_id == None): 
#         first_time_dowloading = True
#         # tweets = api.get_users_tweets(id = userID,
#         #                         user_fields=userID, 
#         #                         max_results =100)
#         tweets = api.user_timeline(screen_name=userID, 
#                                 count=200,
#                                 include_rts = False,
#                                 trim_user = False,
#                                 tweet_mode = 'extended')
#         all_tweets.extend(tweets)
        
#         oldest_id = tweets[-1].id
#         while True :
#             tweets = api.user_timeline(screen_name=userID,
#                                 # 200 is the maximum allowed count
#                                 count=200, 
#                                 include_rts = False,
#                                 max_id = oldest_id - 1,
#                                 trim_user = False,
#                                 tweet_mode = 'extended'
#                                 )
#             if len(tweets) == 0:
#                 break
#             oldest_id = tweets[-1].id
#             all_tweets.extend(tweets)
#     #################################################
#     else:
#         # Start where we ended
#         tweets = api.user_timeline(screen_name=userID, 
#                                 count=200,
#                                 include_rts = False,
#                                 since_id = since_id,
#                                 trim_user = False,
#                                 tweet_mode = 'extended'
#                                 )
#         # if we haven't downloaded today
#         if( len(tweets) != 0 ):
#             all_tweets.extend(tweets)
#             oldest_id = tweets[-1].id
#             since_id = tweets[0].id
             
#             while True :
#                 tweets = api.user_timeline(screen_name=userID,
#                                         # 200 is the maximum allowed count
#                                         count=200, 
#                                         include_rts = False,
#                                         since_id = since_id,
#                                         max_id = oldest_id - 1,
#                                         trim_user = False,
#                                         tweet_mode = 'extended'
#                                         )
#                 if len(tweets) == 0:
#                     break
#                 oldest_id = tweets[-1].id
#                 all_tweets.extend(tweets)
#     #################################################
#     if(len(all_tweets) > 0):
#         outtweets = []
#         for tweet in all_tweets:
            
#             # encode decode
#             txt = tweet.full_text
#             txt = txt.encode("utf-8").decode("utf-8")
            
#             # Pull data from the tweet
#             tweet_list = [
#                 tweet.id_str,
#                 tweet.created_at,
#                 'https://twitter.com/i/web/status/' + tweet.id_str,
#                 tweet.favorite_count, 
#                 tweet.retweet_count,
#                 txt 
#             ]
#             outtweets.append(tweet_list)
#         df_temp = pd.DataFrame(outtweets, 
#                                columns=['id',
#                                         'created_at',
#                                         'url',
#                                         'favorite_count',
#                                         'retweet_count',
#                                         'text'])
        
#         # Pulling specific txt into their own seperate columns
#         s = pd.Series(df_temp.text)
#         website = r'http\S+|www\S+'
#         username = r'@[\w]+'
#         hashtag = r'#[\w]+'
        
#         hashtags = s.str.findall(hashtag, flags=re.IGNORECASE).str.join(" ")
#         usernames = s.str.findall(username, flags=re.IGNORECASE).str.join(" ")
#         links = s.str.findall(website, flags=re.IGNORECASE).str.join(" ")
        
#         emoji_extraction = advertools.extract_emoji(s)
#         emojis = pd.Series(emoji_extraction['emoji']).str.join(" ")
#         emoji_text = pd.Series(emoji_extraction['emoji_text']).str.join(" ")
        
#         df_temp.insert(5, 'hashtags', hashtags)
#         df_temp.insert(6, "emojis", emojis)
#         df_temp.insert(7, "emoji_text", emoji_text)
#         df_temp.insert(8, "usernames", usernames)
#         df_temp.insert(9, "links", links)
        
#         # using dictionary to convert specific columns
#         df_temp = df_temp.astype(dataframe_astypes())
#         # Convert UTC to US/Eastern
#         # df_temp.created_at = df_temp.created_at.dt.tz_convert('US/Eastern')
        
#         # If no previous folder then create folder
#         if not os.path.exists(folder):
#             os.makedirs(folder)
#         # If previous history then merge previous history
#         if os.path.exists(file):
#             df_merge = pd.concat([df_temp, df_history],
#                                 axis = 0,
#                                 join = 'outer',
#                                 names=['id','created_at','url','user','favorite_count',
#                                         'retweet_count','url','hashtags','emojis','emoji_text',
#                                         'usernames','links','text'],
#                                 ignore_index=True)
#             df_merge.to_parquet(file,index=False)
#         # else create file
#         else:
#             df_temp.to_parquet(file,index=False)
            
            
#         # print size of download
#         if(display == 'full'):
#             # if first time downloading
#             if(first_time_dowloading == True):
#                 print(f'-> {len(df_temp)} tweets downloaded', end='\n')
#             else:
#                 print(f'-> {len(df_temp)} tweets downloaded, {len(df_merge)} total tweets', end='\n') 
#     #################################################
#     else:
#         # len() of downloaded file is 0
#         if(display == 'full'):
#             print(f"-> [no recent tweets]",end='\n')   

# ## Download Tweets
#     * 3200 limit, adds to previously downloaded files
for group in twitter_groups:
    print(f"\n{group}:\n")
    # grab all user's from columns with user's
    users = list(user_df[group][user_df[group]!= ''])
    
    folder = f'./data/extracted/raw/twitter/{group}'
    user_download(api=api, 
                  user_list=users, 
                  group=group, 
                  folder=folder, 
                  display='full')
    print(f"")
print('Twitter user download complete')

# *******************************************************
# %%
# Merge all Tweets Together
all_tweets_df = merge_tweets(twitter_groups,
                            folder = f"./data/extracted/raw/twitter/",
                            merge = f"./data/extracted/merged/twitter/groups/",
                            all_merge = f"./data/extracted/merged/twitter/")
print(f"size of merged tweets file: {all_tweets_df.shape}\n")
# %%
