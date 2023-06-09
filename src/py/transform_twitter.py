# %% [markdown]
## Data Curation
#   Summary Overview
#   - Vectorize word similarity Ngram and Word2Vec
#   - Normalize and clean data
#   - Create dictionaries from words spoken
#   - Create probabilities of words used per user

# %% [markdown]
# - Import Libraries
from collections import defaultdict
import os, pandas as pd, numpy as np, string, sys, nltk, re
from gensim.models import Word2Vec, KeyedVectors
# import skfda # fuzzy c-means
pd.set_option('display.float_format', str)

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
        # add path to system path for running in terminal
        if(os.getcwd() not in sys.path):
            sys.path.append(os.getcwd())
    except Exception as e:
        print(f"cwd: {os.getcwd()}", sep = '\n')
        print(f"{e}\n:Please start current working directory from {top_level_folder}")
        
# %% [markdown]
# - Load tools
from transform_tools import clean_text, df_to_parquet, n_gram, unigram_probability, bigram_probability, normalize_columns

# %% [markdown]
# # Load Twitter Usernames   
# * Accounts may be privated or removed and change ability to download
# * No two users can have the same id
with open(os.path.normpath(os.getcwd() + '/user_input/twitter_users.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
twitter_groups = list(user_df.columns)


# %% [markdown]
# - Non significant words to remove
nonessential_words = ['twitter', 'birds','lists','list', 'source', 'am', 'pm', 'nan'] + list(string.ascii_lowercase)
stopwords = nltk.corpus.stopwords.words("english") + nonessential_words
words_to_remove = sorted(list( dict.fromkeys(stopwords) )) # remove duplicates

# %%
# Read in Raw Tweets
twitter_df = pd.read_parquet('./data/extracted/merged/twitter/all_twitter.parquet', 
                             engine= 'pyarrow',
                             dtype_backend = 'pyarrow')
# twitter_df = pd.read_parquet('./data/extracted/merged/all_twitter.parquet').astype(dataframe_astypes())

# %% [markdown]
# - Cleaning up tweet sentences -> websites(html/www) -> usernames | hashtags | digits -> extra spaces | stopwords | emoji
# - punctuation (texthero) -> translate to english (in development) -> stemming similar words (e.g.. ['like' 'liked' 'liking'] to ['lik' 'lik' 'lik'])

cleaned_stem_text, cleaned_nonstem_text = clean_text(twitter_df.text, words_to_remove)
cleaned_df = twitter_df.copy()

# %%
cleaned_df['cleaned_stem_text'] = cleaned_stem_text
cleaned_df['cleaned_nonstem_text'] = cleaned_nonstem_text
df_to_parquet(df = cleaned_df, 
            folder = f'./data/transformed/twitter', 
            file = f'/cleaned_twitter.parquet')

# %% [] 
# word2vec Word and sentence vector similarity
# model = Word2Vec(cleaned_nonstem_text,size=100, min_count=1)
# model.save("word2vec.model")
# model = Word2Vec.load("word2vec.model")
# word_vectors = model.wv
# word_vectors.save("word2vec.wordvectors")
# wv = KeyedVectors.load("word2vec.wordvectors", mmap='r')

# %% []

# - cluster similar words
# - kmeans | PCA | fuzzycmeans | tsne
# fuzzy_c = skfda.ml.clustering.FuzzyCMeans(random_state=0)
# fuzzy_c.fit(emb_df)

# %% [markdown]
# - Generate N gram, frequency, and relative frequency
# - Note: Avoided N gram probability matrix -> ran into memory constraints
print(f"Extracting ngram Sentences:\n")
ngram_words= cleaned_stem_text.copy()
unigram_sentence, unigram_frequency, unigram_relative_frequency = n_gram(ngram_words, 1)
bigram_sentence, bigram_frequency, bigram_relative_frequency = n_gram(ngram_words, 2)
trigram_sentence, trigram_frequency, trigram_relative_frequency = n_gram(ngram_words, 3)
quadgram_sentence, quadgram_frequency, quadgram_relative_frequency = n_gram(ngram_words, 4)
pentagram_sentence, pentagram_frequency, pentagram_relative_frequency = n_gram(ngram_words, 5)
print(f"Finished ngram Extraction:\n")

# %% [markdown]
# N Gram probabilities (~ 30 minutes) -> [To improve could apply laplase Smoothing] / [skipgrams]
# $$ Unigram = P(W_{1:n})= \prod_{k=1}^n P(W_{k}) $$
# $$ Bigram = P(W_{1:n})\approx\prod_{k=1}^n P(W_{k}|W_{k-1}) $$
# $$ P(W_{n}|W_{n-1}) =  \dfrac{Count(W_{n-1}W{n})}{Count(W{n-1})} $$
# $$ Trigram = P(W_{1:n})\approx\prod_{k=1}^n P(W_{k}|W_{{k-2}, W_{k-1}}) $$
# ******************************************* Goal to optimize with pyArrow *******************************************
print(f"Calculating Unigram Probability:\n")
unigram_prob = unigram_probability(cleaned_stem_text, unigram_relative_frequency)
print(f"Finished Unigram Probability:\n")
# %%
print(f"Calculating Bigram Probability:\n")
bigram_prob = bigram_probability(cleaned_stem_text, bigram_sentence, unigram_frequency, bigram_frequency)
print(f"Finished Bigram Probability:\n")

# %% [markdown]
# - Combine ngram probability and cleaned text variations to dataframe
cleaned_df_ngram = cleaned_df.copy()
cleaned_df_ngram['bigram_sentence'] = bigram_sentence.reset_index(drop=True).astype("string[pyarrow]")
cleaned_df_ngram['trigram_sentence'] = trigram_sentence.reset_index(drop=True).astype("string[pyarrow]")
cleaned_df_ngram['quadgram_sentence'] = quadgram_sentence.reset_index(drop=True).astype("string[pyarrow]")
cleaned_df_ngram['pentagram_sentence'] = pentagram_sentence.reset_index(drop=True).astype("string[pyarrow]")
cleaned_df_ngram['unigram_probability'] = unigram_prob.reset_index(drop=True)
cleaned_df_ngram['bigram_probability'] = bigram_prob
# Converting timestamp (HH:MM:SS) to Year-month-day to combine users on the same day
cleaned_df_ngram.insert(loc = 0, column = 'date', value = pd.to_datetime(cleaned_df_ngram['created_at']).apply(lambda x: x.strftime('%Y-%m-%d')))
cleaned_df_ngram.date = pd.to_datetime(cleaned_df_ngram['date'], format='%Y-%m-%d') # object to datetime64[ns] -> or to datetime64[ns, UTC] 
cleaned_df_ngram = cleaned_df_ngram.sort_values(by=['date'], ascending=False)
# exporting
df_to_parquet(df = cleaned_df_ngram, 
            folder = f'./data/transformed/twitter', 
            file = f'/cleaned_twitter_ngram.parquet')

# %%
# 2nd Half pivots and normalization
# - normalize probability column to 1 
df_all_prob_norm = cleaned_df_ngram.copy()
df_all_prob_norm.unigram_probability = cleaned_df_ngram.unigram_probability / cleaned_df_ngram.unigram_probability.sum()
df_all_prob_norm.bigram_probability = cleaned_df_ngram.bigram_probability / cleaned_df_ngram.bigram_probability.sum()

# normalize min/max favorite_count and retweet_count
columns = ['favorite_count','retweet_count']
for c in columns:
    df_all_prob_norm[c] = (df_all_prob_norm[c] - df_all_prob_norm[c].min()) / (df_all_prob_norm[c].max() - df_all_prob_norm[c].min())
    
# - Threshold tweets
if False:
    threshold = '2017-01-01'
    df_all_prob_norm = df_all_prob_norm[df_all_prob_norm.created_at > threshold]
    
# %%
# Any Inter quantile data to remove?
# Upper 95% or lower 5% | Extrema

df_to_parquet(df = df_all_prob_norm, 
          folder = f'./data/transformed/twitter', 
          file = f'/cleaned_twitter_ngram_norm.parquet')

# %% 
# Wide pivot Users by day

def wide_pivot_byuser(df, index, probabilities, file, folder, other_numeric=None, agg='sum'):
    
    aggfunc = dict(zip(probabilities, [agg] * (len((probabilities)))))
    df_user_prob = df.pivot_table(index=index, 
                                   columns=['user'],
                                   values=probabilities, 
                                   aggfunc=aggfunc,
                                   fill_value=0 ).sort_values(by=index,
                                                              ascending=False)
    # fix column names of which user and probability
    df_user_prob.columns = pd.Series(['_'.join(str(s).strip() for s in col if s) for col in df_user_prob.columns]).str.replace("probability_", "", regex=True)
    if(other_numeric is not None):
        df_other_numeric = df.pivot_table(index=index, values=other_numeric, aggfunc=agg,fill_value=0).sort_values(by=index, ascending=False)
        df = pd.merge(df_user_prob, df_other_numeric, how='inner', on= index).reset_index()
        df_to_parquet(df = df, 
                folder = folder, 
                file = file)
        return df

    else:
        df = df_user_prob
        df_to_parquet(df = df, 
                folder = folder, 
                file = file)
        return df
    
other_numeric = ['favorite_count','retweet_count']
probability_types = [match for match in list(df_all_prob_norm.columns) if "probability" in match]

df_wide_byday = wide_pivot_byuser(df = df_all_prob_norm.copy(), 
                  index='date', 
                  probabilities = probability_types,
                  file = f'/pivot_user_byday.parquet',
                  folder = f'./data/transformed/twitter',
                  other_numeric = other_numeric,
                  agg = 'sum')
# created_at is currently timestamp[us, tz=UTC][pyarrow]

# %%
# merge by frequency

def frequency_merge(df, numeric_columns, freq="30min", agg='sum', index='created_at', offset=None):
    
    all_columns = numeric_columns + ['user', index]
    df[index] = pd.to_datetime(df[index])
    aggregation = dict(zip(numeric_columns,[agg]*len(numeric_columns)))
    df = df.loc[:,all_columns]
    # group by numeric values
    df = df.groupby(['user', pd.Grouper(key=index, 
                                        freq=freq, 
                                        dropna=False, 
                                        offset=offset)]).agg(aggregation).reset_index()
    return df

def frequency_fill(df, freq, fill, index):
    
    # Fill All Dates
    df = df.set_index(index).asfreq(freq=freq, fill_value=fill).reset_index()
    return df

def fill_time(df, freq, hour, minute, fill, index):
    
    # Fill Opening Hour Dates
    start = df[index].min().normalize().replace(hour=hour,minute=minute)
    end = df[index].max().normalize().replace(hour=hour,minute=minute)
    # every twitter timestamp 
    all_days = df.set_index(index).index
    # twitter timestamps for Opening Hours
    start_days = pd.date_range(start=start, end=end, freq=freq, name=index)
    filled_index = all_days.union(start_days)
    
    df = df.set_index(index).reindex(filled_index).fillna(fill).reset_index()
    
    return df

numeric_columns = probability_types + other_numeric
# ***** BY HOUR *****
df_all_prob_norm_byhour = frequency_merge(df=df_all_prob_norm.copy(), 
                                          numeric_columns = numeric_columns, 
                                          freq="H", 
                                          agg='sum', 
                                          index='created_at',
                                          offset='30min')
df_wide_byhour = wide_pivot_byuser(df = df_all_prob_norm_byhour, 
                                        index='created_at', 
                                        probabilities = probability_types,
                                        file = f'/pivot_user_byhour.parquet',
                                        folder = f'./data/transformed/twitter',
                                        other_numeric = other_numeric,
                                        agg = 'sum')

df_wide_byhour = fill_time(df_wide_byhour, freq="D", hour=13, minute=30, fill=0, index ='created_at')
# print(df_wide_byhour.created_at.dt.date.value_counts())
# ***** BY HALF HOUR *****
df_all_prob_norm_by_halfhour = frequency_merge(df=df_all_prob_norm.copy(), 
                                          numeric_columns = numeric_columns, 
                                          freq="30min", 
                                          agg='sum', 
                                          index='created_at')
df_wide_by_halfhour = wide_pivot_byuser(df = df_all_prob_norm_by_halfhour, 
                                        index='created_at', 
                                        probabilities = probability_types,
                                        file = f'/pivot_user_by_halfhour.parquet',
                                        folder = f'./data/transformed/twitter',
                                        other_numeric = other_numeric,
                                        agg = 'sum')
df_wide_by_halfhour = fill_time(df_wide_by_halfhour, freq="D", hour=13, minute=30, fill=0, index ='created_at')
# ***** BY five MINUTES *****
df_all_prob_norm_by_five_minutes = frequency_merge(df=df_all_prob_norm.copy(), 
                                          numeric_columns = numeric_columns, 
                                          freq="5min", 
                                          agg='sum', 
                                          index='created_at')
df_wide_by_five_minutes = wide_pivot_byuser(df = df_all_prob_norm_by_five_minutes, 
                                        index='created_at', 
                                        probabilities = probability_types,
                                        file = f'/pivot_user_by_five_minutes.parquet',
                                        folder = f'./data/transformed/twitter',
                                        other_numeric = other_numeric,
                                        agg = 'sum')
df_wide_by_five_minutes = fill_time(df_wide_by_five_minutes, freq="D", hour=13, minute=30, fill=0, index ='created_at')
# ***** BY MINUTE *****
df_all_prob_norm_by_minute = frequency_merge(df=df_all_prob_norm.copy(), 
                                          numeric_columns = numeric_columns, 
                                          freq="1min", 
                                          agg='sum', 
                                          index='created_at')
df_wide_by_minute = wide_pivot_byuser(df = df_all_prob_norm_by_minute, 
                                        index='created_at', 
                                        probabilities = probability_types,
                                        file = f'/pivot_user_by_minute.parquet',
                                        folder = f'./data/transformed/twitter',
                                        other_numeric = other_numeric,
                                        agg = 'sum')
df_wide_by_minute = frequency_fill(df = df_wide_by_minute.copy(), 
                                   freq = '1min', 
                                   fill=0, 
                                   index='created_at')
# %% [markdown]
# Combine Weekend Data

def merge_days(df, merge_days, merge_freq_on, index, file, folder, offset=None):
    mask = df[index].dt.day_name().isin(merge_days)
    selected_days = df.loc[mask, :]
    
    # Days over the weekend
    merged_selection = selected_days.groupby([pd.Grouper(key=index, 
                                                        freq=merge_freq_on, 
                                                        dropna=False, 
                                                        offset=offset)]).sum().reset_index(index)
    # Combine Days e.g.( Mon = Mon+Sat+Sun )
    df_other_days = df.reset_index(drop=True).loc[~ mask, :]
    df_output = pd.merge(df_other_days, merged_selection, how='outer').set_index(index).sort_index(ascending=False).reset_index()
    
    df_to_parquet(df = df_output, 
                  folder = folder, 
                  file = file)
    return df_output

def merge_days_byhour(df, merge_days, merge_freq_on, hour, minute, index, file, folder, offset=None):
    
    # Days in Sat Sun
    mask = df[index].dt.day_name().isin(merge_days)
    selected_days = df.loc[mask, :]
    
    # Merged Sat + Sun  = Mon
    merged_selection = selected_days.groupby([pd.Grouper(key=index, 
                                                        freq=merge_freq_on)]).sum().reset_index(index)
    # Changed hours to Opening Hours
    merged_selection[index] = merged_selection[index].apply(lambda x:x.replace(hour=hour,minute=minute))
    
    # All other Days Mon-Fri
    df_other_days = df.reset_index(drop=True).loc[~ mask, :]

    # Merge Mon-Fri with Sat+Sun=Mon
    # monday_merge = pd.merge(merged_selection, df_other_days, on=index, how='outer')
    monday_merge = pd.concat([merged_selection.set_index(index), df_other_days.set_index(index)])
    
    # Sum Sat+Sun with Monday
    df_output = monday_merge.groupby(index).sum().sort_index(ascending=False).reset_index()
    
    df_to_parquet(df = df_output, 
                  folder = folder, 
                  file = file)
    return df_output

# Weekend Merge (Mon = Sat+Sun+Mon Tweets )
# ***** BY DAY *****
wkd_merge_byday = merge_days(df = df_wide_byday.copy(),
                            merge_days = ['Saturday', 'Sunday', 'Monday'], 
                            merge_freq_on= 'W-MON',
                            index= 'date',
                            file=f'/pivot_user_wkd_merge_byday.parquet',
                            folder=f'./data/transformed/twitter')
# ***** BY HOUR *****
wkd_merge_byhour = merge_days_byhour(  df = df_wide_byhour.copy(),
                                merge_days = ['Saturday', 'Sunday'], 
                                merge_freq_on= 'W-MON',
                                hour = 13,
                                minute= 30,
                                index= 'created_at',
                                file=f'/pivot_user_wkd_merge_byhour.parquet',
                                folder=f'./data/transformed/twitter')
# ***** BY HALF HOUR *****
wkd_merge_by_halfhour = merge_days_byhour(df = df_wide_by_halfhour.copy(),
                                    merge_days = ['Saturday', 'Sunday'], 
                                    merge_freq_on= 'W-MON',
                                    hour = 13,
                                    minute= 30,
                                    index= 'created_at',
                                    file=f'/pivot_user_wkd_merge_by_halfhour.parquet',
                                    folder=f'./data/transformed/twitter')
# ***** BY FIVE MINUTES *****
wkd_merge_by_five_minutes = merge_days_byhour(df = df_wide_by_five_minutes.copy(),
                                    merge_days = ['Saturday', 'Sunday'], 
                                    merge_freq_on= 'W-MON',
                                    hour = 13,
                                    minute= 30,
                                    index= 'created_at',
                                    file=f'/pivot_user_wkd_merge_by_five_minutes.parquet',
                                    folder=f'./data/transformed/twitter')
# ***** BY MINUTE *****
wkd_merge_by_minute = merge_days_byhour(df = df_wide_by_minute.copy(),
                                    merge_days = ['Saturday', 'Sunday'], 
                                    merge_freq_on= 'W-MON',
                                    hour = 13,
                                    minute= 30,
                                    index= 'created_at',
                                    file=f'/pivot_user_wkd_merge_by_minute.parquet',
                                    folder=f'./data/transformed/twitter')
# %% [markdown]
# Working Hour Range Merge

def merge_hours(df, day_start, day_end, wrk_start, wrk_end, hour, minute, index, file, folder):

    # UST to EST timezone ( Merge on the correct days )
    df[index] = pd.to_datetime(df[index].dt.tz_convert('America/New_York'))
    
    # Tweets split up by hours of the day
    wrk_hours = df.set_index(index).between_time(start_time= wrk_start, end_time= wrk_end).reset_index()
    before_wrk = df.set_index(index).between_time(start_time= day_start, end_time= wrk_start).groupby(pd.Grouper(freq='D')).sum().reset_index()
    after_wrk = df.set_index(index).between_time(start_time= wrk_end, end_time= day_end).groupby(pd.Grouper(freq='D')).sum().reset_index()
    
    # Merge before work hours with opening hour
    before_wrk[index] = before_wrk[index].apply(lambda x:x.replace(hour=hour,minute=minute))
    wrk_hrs_merge = pd.concat([wrk_hours, before_wrk]).groupby(index).sum()

    # Merge after work hours with next day opening hour
    # e.g., ([2023-02-27 09:30 values now shift to 2023-02-28 09:30] and sum with original opening hours values on [2023-02-28 09:30])
    after_wrk[index] = after_wrk[index].apply(lambda x:x.replace(hour=hour,minute=minute))
    after_wrk_shifted= after_wrk.set_index(index).shift(freq='D', periods=1)
    
    wrk_hrs_merge = pd.concat([wrk_hrs_merge, after_wrk_shifted])
    wrk_hrs_merge = wrk_hrs_merge.groupby(index).sum().reset_index()

    wrk_hrs_merge = wrk_hrs_merge.set_index(index).astype('float64[pyarrow]').reset_index()
    wrk_hrs_merge[index] = wrk_hrs_merge[index].dt.tz_convert('America/New_York')
    wrk_hrs_merge = wrk_hrs_merge.rename(columns={index:'timestamp'})
    
    df_to_parquet(df = wrk_hrs_merge, 
            folder = folder, 
            file = file)
# stock market hours = (9:30-4PM EST -> 6:30-1:00PM PST -> 13:30-20:00 UTC)
# # ***** BY HOUR *****
merge_hours(df=wkd_merge_byhour.copy(),
            day_start ='00:00',
            wrk_start = '09:30',
            wrk_end = '15:30',
            day_end = '23:59',
            hour = 9,
            minute = 30,
            index='created_at',
            file=f'/pivot_user_wkd_merge_byhour_wrkhrs.parquet',
            folder=f'./data/transformed/twitter')
# ***** BY HALF HOUR *****
merge_hours(df=wkd_merge_by_halfhour.copy(),
            day_start ='00:00',
            wrk_start = '09:30',
            wrk_end = '15:30',
            day_end = '23:59',
            hour = 9,
            minute = 30,
            index='created_at',
            file=f'/pivot_user_wkd_merge_by_halfhour_wrkhrs.parquet',
            folder=f'./data/transformed/twitter')
# ***** BY FIVE MINUTES *****
merge_hours(df=wkd_merge_by_five_minutes.copy(),
            day_start ='00:00',
            wrk_start = '09:30',
            wrk_end = '15:30',
            day_end = '23:59',
            hour = 9,
            minute = 30,
            index='created_at',
            file=f'/pivot_user_wkd_merge_by_five_minutes_wrkhrs.parquet',
            folder=f'./data/transformed/twitter')
# ***** BY MINUTE *****
merge_hours(df=wkd_merge_by_minute.copy(),
            day_start ='00:00',
            wrk_start = '09:30',
            wrk_end = '15:30',
            day_end = '23:59',
            hour = 9,
            minute = 30,
            index='created_at',
            file=f'/pivot_user_wkd_merge_by_minute_wrkhrs.parquet',
            folder=f'./data/transformed/twitter')
# %%