# Changelog
This file contains the notable changes to the project

Version 1.0.0 (01-10-2023)
## New
    - Automated tweet preperation and processing of incoming long-term and short-term 
        financial analyst tweets using Tweepy's (api.user_timeline())
        This method is limited to ~ 3k tweets per user eg.(JimCramer has 123k tweets 
        and I downloaded 2950)
    - Added probability of words spoken per tweet (set sum of probability column == 1)
    - Added merged columns of long-term and short term financial analysts to index funds 
        including ( id, created_at, user, url, text, frequency, probability     
        favorite_count, retweet, count, SandP500, NASDAQ, Dow_JONES, RUSSEL )
    - Added a Linear Model to predict retweet_count as a placeholder for future stock
        market prediction

## Changes 
    - Specified the dataframe Datetime for integar string and datetime variables, 
        deciding to use UTC for Universal time zone
        {'id': 'int64',
        'created_at': 'datetime64[ns, UTC]',
        'user':'object',
        'favorite_count': 'int64',
        'retweet_count': 'int64',
        'url': 'object',
        'text': 'object'}
    - Removal of whitespace, and stop/filler words from dictionary of all words 
        spoken in twitter messages with regex and pandas filtering

## Deprecated
    - older versions of the spreadsheets have ' and misaligned spaces

## Removed
    - Binary target y created from day to day increase or decrease in index fund
        value
    - Removed some of the twitter csv files github unable to push files
        547.84 MB; this exceeds GitHub's file size limit of 100.00 MB
## Fixes
    - Fixed misnamed csv files

## Security
    - may need to sensor the twitter data in future versions


Version 1.0.1 (01-25-2023)

## Changes 
    - Updated File Structure of project
        Sentiment_Analysis
            Stock_Market
                authentication
                data
                notebook
                    TwitterBot.ipynb
                src
                    preparation
                        tweepy_functions.py
                        __init__.py
                    __init__.py
                __init__.py
## New
    - nested '__init__.py' files to organize tweepy_functions in preparation stage

Version 1.0.2 (01-30-2023)
## New

    - Added Excel file to download any user in the file with correct twitter tag
        Any column added will be downloaded as seperate csv files
    - Cleaned up and seperated code, moving train functions into src directory

## Changes 

    - min max normalized the index funds and like/favorite counts
    - predicted market for all index funds running seperate linear models

Version 1.0.3 (02-13-2023)
## New

    - Revamped the project from Sentiment Analysis to Data Engineering pipeline
    - Seperated code into different data engineering parts
    design, parse, curate, condition, load, deploy

## Deprecated

    - Removed Analysis, and changed project focus

## Upcoming
    - Attempting to deploy an executable of the nested py files pyinstaller with anaconda ( running into library issues )
    - Plan on integrating Apache Spark in the load phase to utilized the optimized SQL facets ( need to finish seting up the Apache Spark download and will run from Python using Pyspark)
    - Plan on chaning the Twitter downloads to append to previous csv files, and not redownload historical data
    - Plan on chaning the Twitter downloads to append to previous csv files, and not redownload historical data
    - Also plan on deploying as a docker file aswell

Version 1.0.4 (03-14-2023)
    
## New

    - Tweet bigram and Unigram probabilites
    - Loaded into dynamodb Tall database for final csv

## Upcoming
    
    - Pandas 2.0 integration with pyarrow 30x performance increase
    - Update csv UTF-8 files to parquet binary files for faster reading/writing and smaller downloads
    - English translation with EasyNMT
    - Compression/encryption
    - word2vec and kmeans/PCA clustering of Ngram probabilities
    - Trigram probabilites
    - Complete load of all datasets
    - Complete exe/Docker deployment
    - Load wide database Apache cassandra
    - Build a relational database

Version 1.0.5 (05-09-2023)

## Deprecated

    - Migrating code from previous twitter_ETL code to prepare to merge with other social media platforms
    - Removed the transformations, loads, and changed overall hierarechal structure of code
    - Removing csv files, and Yahoo Finance connections with tweets

## New

    - Connected the Load scripts to MYSQL database with hashed credentials 
      (argon2id)
    - Created the first schema in "socialmedia" database for the "TwitterRaw" 
      table in order to load the merged parquet file

## Upcoming

    - Learned how to correctly use the Dockerfile to individually dockerize 
    python and MYSQL databases
    - Plan to use docker-compose to bridge the gap between the python 
      docker file and SQL docker file for an automated database creation 
      on any operating system
    - Finish loading TwitterRaw and TwitterClean tables into MYSQL, 
      and finish creating the surrogate keys (PK/FK) to connect and seperate
      Date and User dimensional table from facts