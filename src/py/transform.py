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
from transform_tools import clean_text, df_to_parquet, n_gram, unigram_probability, bigram_probability, dataframe_astypes

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
nonessential_words = ['twitter', 'birds','lists','list', 'source', 'am', 'pm', 'nan'] + list(string.ascii_lowercase) + list(string.ascii_uppercase)
stopwords = nltk.corpus.stopwords.words("english") + nonessential_words
words_to_remove = sorted(list( dict.fromkeys(stopwords) )) # remove duplicates

# Read in Raw Tweets
twitter_df = pd.read_parquet('./data/extracted/merged/all_twitter.parquet').astype(dataframe_astypes())

# %% [markdown]
# - Cleaning up tweet sentences -> websites(html/www) -> usernames | hashtags | digits -> extra spaces | stopwords | emoji
# - punctuation (texthero) -> translate to english (in development) -> stemming similar words (e.g.. ['like' 'liked' 'liking'] to ['lik' 'lik' 'lik'])
cleaned_stem_text, cleaned_nonstem_text = clean_text(twitter_df.text, words_to_remove)
cleaned_df = twitter_df.copy()
cleaned_df['cleaned_stem_text'] = cleaned_stem_text
cleaned_df['cleaned_nonstem_text'] = cleaned_nonstem_text
df_to_parquet(df = cleaned_df, 
            folder = f'./data/transformed', 
            file = f'/cleaned_twitter.parquet')

# %% [markdown] 
# word2vec Word and sentence vector similarity
# model = Word2Vec(cleaned_nonstem_text,size=100, min_count=1)
# model.save("word2vec.model")
# model = Word2Vec.load("word2vec.model")
# word_vectors = model.wv
# word_vectors.save("word2vec.wordvectors")
# wv = KeyedVectors.load("word2vec.wordvectors", mmap='r')

# %% [markdown]
# - cluster similar words
# - kmeans | PCA | fuzzycmeans | tsne
# fuzzy_c = skfda.ml.clustering.FuzzyCMeans(random_state=0)
# fuzzy_c.fit(emb_df)

# %% [markdown]
# - Generate N gram, frequency, and relative frequency
# - Note: Avoided N gram probability matrix -> ran into memory constraints
unigram_sentence, unigram_frequency, unigram_relative_frequency = n_gram(cleaned_stem_text, 1)
bigram_sentence, bigram_frequency, bigram_relative_frequency = n_gram(cleaned_stem_text, 2)
trigram_sentence, trigram_frequency, trigram_relative_frequency = n_gram(cleaned_stem_text, 3)
quadgram_sentence, quadgram_frequency, quadgram_relative_frequency = n_gram(cleaned_stem_text, 4)
pentagram_sentence, pentagram_frequency, pentagram_relative_frequency = n_gram(cleaned_stem_text, 5)

# %% [markdown]
# N Gram probabilities (~ 30 minutes)
# $$ Unigram = P(W_{1:n})= \prod_{k=1}^n P(W_{k}) $$
# $$ Bigram = P(W_{1:n})\approx\prod_{k=1}^n P(W_{k}|W_{k-1}) $$
# $$ P(W_{n}|W_{n-1}) =  \dfrac{Count(W_{n-1}W{n})}{Count(W{n-1})} $$
# $$ Trigram = P(W_{1:n})\approx\prod_{k=1}^n P(W_{k}|W_{{k-2}, W_{k-1}}) $$
# - To improve could apply laplase Smoothing / skipgrams 
unigram_prob = unigram_probability(cleaned_stem_text, unigram_relative_frequency)
bigram_prob = bigram_probability(cleaned_stem_text, bigram_sentence, unigram_frequency, bigram_frequency)

# %% [markdown]
# - Combine ngram probability and cleaned text variations to dataframe
cleaned_df_ngram = cleaned_df.copy()
cleaned_df_ngram['bigram_sentence'] = bigram_sentence.reset_index(drop=True)
cleaned_df_ngram['trigram_sentence'] = trigram_sentence.reset_index(drop=True)
cleaned_df_ngram['quadgram_sentence'] = quadgram_sentence.reset_index(drop=True)
cleaned_df_ngram['pentagram_sentence'] = pentagram_sentence.reset_index(drop=True)
cleaned_df_ngram['unigram_probability'] = unigram_prob.reset_index(drop=True)
cleaned_df_ngram['bigram_probability'] = bigram_prob
# %%
# Converting timestamp (HH:MM:SS) to Year-month-day to combine users on the same day
cleaned_df_ngram.insert(loc = 0, column = 'date', value = pd.to_datetime(cleaned_df_ngram['created_at']).apply(lambda x: x.strftime('%Y-%m-%d')))
cleaned_df_ngram.date = pd.to_datetime(cleaned_df_ngram['date'], format='%Y-%m-%d') # object to datetime64[ns] -> or to datetime64[ns, UTC]
# %%
cleaned_df_ngram = cleaned_df_ngram.sort_values(by=['date'], ascending=False)
df_to_parquet(df = cleaned_df_ngram, 
            folder = f'./data/transformed', 
            file = f'/cleaned_twitter_ngram.parquet')
# %%