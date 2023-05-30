# %% [markdown]
## Data Curation
#   Summary Overview
#   - Vectorize word similarity Ngram and Word2Vec
#   - Normalize and clean data
#   - Create dictionaries from words spoken
#   - Create probabilities of words used per user

# %% [markdown]
# - Import Libraries
import os, pandas as pd, numpy as np, string, sys, nltk
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
twitter_df = pd.read_parquet('./data/extracted/merged/all_twitter.parquet', 
                             engine= 'pyarrow',
                             dtype_backend = 'pyarrow')
# twitter_df = pd.read_parquet('./data/extracted/merged/all_twitter.parquet').astype(dataframe_astypes())

# %% [markdown]
# - Cleaning up tweet sentences -> websites(html/www) -> usernames | hashtags | digits -> extra spaces | stopwords | emoji
# - punctuation (texthero) -> translate to english (in development) -> stemming similar words (e.g.. ['like' 'liked' 'liking'] to ['lik' 'lik' 'lik'])
# %%
cleaned_stem_text, cleaned_nonstem_text = clean_text(twitter_df.text, words_to_remove)
cleaned_df = twitter_df.copy()

# %%
cleaned_df['cleaned_stem_text'] = cleaned_stem_text
cleaned_df['cleaned_nonstem_text'] = cleaned_nonstem_text
df_to_parquet(df = cleaned_df, 
            folder = f'./data/transformed', 
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
            folder = f'./data/transformed', 
            file = f'/cleaned_twitter_ngram.parquet')

# %%
# 2nd Half pivots and normalization
# - normalize probability column to 1 
df_all_prob_norm = cleaned_df_ngram.copy()
df_all_prob_norm.unigram_probability = cleaned_df_ngram.unigram_probability / cleaned_df_ngram.unigram_probability.sum()
df_all_prob_norm.bigram_probability = cleaned_df_ngram.bigram_probability / cleaned_df_ngram.bigram_probability.sum()

# - Threshold tweets
if False:
    threshold = '2017-01-01'
    df_all_prob_norm = df_all_prob_norm[df_all_prob_norm.created_at > threshold]
# %%
# Any Inter quantile data to remove?
# Upper 95% or lower 5%| Extrema

df_to_parquet(df = df_all_prob_norm, 
          folder = f'./data/transformed', 
          file = f'/cleaned_twitter_ngram_norm.parquet')

# %% Merge Users on same dates
df_wide1 = df_all_prob_norm.pivot_table(index='date', 
                                   values=['favorite_count','retweet_count'], 
                                   aggfunc='sum',
                                   fill_value=0).sort_values(by='date',
                                                            ascending=False)
df_wide2 = df_all_prob_norm.pivot_table(index='date', 
                                   columns=['user'],
                                   values=['unigram_probability','bigram_probability'], 
                                   aggfunc={'unigram_probability': 'sum',
                                            'bigram_probability': 'sum'},
                                   fill_value=0 ).sort_values(by='date',
                                                              ascending=False)#.droplevel(0, axis=1) 
df_wide2.columns = pd.Series(['_'.join(str(s).strip() for s in col if s) for col in df_wide2.columns]).str.replace("probability_", "", regex=True)
df_wide = pd.merge(df_wide1, df_wide2, how='inner', on='date').reset_index()
df_to_parquet(df = df_wide, 
          folder = f'./data/transformed', 
          file = f'/pivot_user_by_date.parquet')

# %% [markdown]
# - To combine Sat/Sun Tweets with Monday
week_end_mask = df_wide.date.dt.day_name().isin(['Saturday', 'Sunday', 'Monday'])
week_end = df_wide.loc[week_end_mask, :]
monday_group = week_end.groupby([pd.Grouper(key='date', freq='W-MON')]).sum().reset_index('date')
# Apply the stripped mask
df_wide_stripped = df_wide.reset_index().loc[~ week_end_mask, :]
df_wide_wknd_merge = pd.merge(df_wide_stripped, monday_group, how='outer').set_index('date').sort_index(ascending=False).reset_index()
df_to_parquet(df = df_wide_wknd_merge, 
          folder = f'./data/transformed', 
          file = f'/pivot_user_by_date_wkd_merge.parquet')
# %%