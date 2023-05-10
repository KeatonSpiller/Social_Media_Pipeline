# %% [markdown]
## Data Curation
#   Summary Overview
#   - Merge Parsed Tweets
#   - Cutoff tweets older than 2017
#   - Normalize and clean data
#   - Create dictionaries from words spoken
#   - Create probabilities of words used per user

# %% [markdown]
# - Import Libraries
import os, nltk, pandas as pd, numpy as np, skfda, string
from gensim.models import Word2Vec
from nltk.stem.snowball import SnowballStemmer

# %% [markdown]
# - Change Directory to top level folder
top_level_folder = 'twitter_app'
if(os.getcwd().split(os.sep)[-1] != top_level_folder):
    try:
        os.chdir('../..')
        print(f"cwd: {os.getcwd()}", sep = '\n')
    except Exception as e:
        print(f"{e}\n:start current working directory from {top_level_folder}")
        
# %% [markdown]
# - Load tools
from src.curate.curate_tools import clean_text, merge_files, merge_all, df_to_csv, n_gram, unigram_probability, bigram_probability

# %% [markdown]
# - Read in groups of twitter user's to merge
with open(os.path.normpath(os.getcwd() + './user_input/user_list.xlsx'), 'rb') as f:
    user_df = pd.read_excel(f, sheet_name='user_names')
    user_df = user_df.where(pd.notnull(user_df), '')
    f.close()
groups = list(user_df.columns)
user_df
    
# %% [markdown]
# - Merge Tweets
for group in groups:
    merge_files(group, display = 0)
df_all = merge_all(display = 0)

# %% [markdown]
# - Top 5 oldest tweets by user
df_all.groupby('user')['created_at'].min().sort_values(ascending= True).head(5)

# %% [markdown]
# - Non significant words to remove
nonessential_words = ['twitter', 'birds','lists','list', 'source', 'am', 'pm', 'nan'] + list(string.ascii_lowercase) + list(string.ascii_uppercase)
stopwords = nltk.corpus.stopwords.words("english") + nonessential_words
words_to_remove = sorted(list( dict.fromkeys(stopwords) )) # remove duplicates

# %% [markdown]
# - Cleaning up tweet sentences
# - websites(html/www)
# - usernames | hashtags | digits
# - extra spaces | stopwords | emoji
# - punctuation (texthero)
# - translate to english -> in development
# - stemming similar words -> 'like' 'liked' 'liking' to stem:'lik'

cleaned_stem_text, cleaned_nonstem_text = clean_text(df_all.text, words_to_remove)

df_to_csv(df = cleaned_stem_text, 
          folder = f'./data/merge/all_twitter_users/stats', 
          file = f'/cleaned_stem_text.csv')
df_to_csv(df = cleaned_nonstem_text, 
          folder = f'./data/merge/all_twitter_users/stats', 
          file = f'/cleaned_nonstem_text.csv')

# %% [markdown] 
# - Word and sentence vector similarity
# - Word2vec | BERT | Tf-Idf
# word_vector = Word2Vec(cleaned_text,size=100, min_count=1)

# %% [markdown]
# - cluster similar words
# - kmeans | PCA | fuzzycmeans | tsne
# - time complexity of PCA ( O(p2n+p3) ) | k-means O(ndk+1 log n)
# - kmeans and PCA used for large datasets to compress/group/simplify the data
# - kmeans creates a hard line and fuzzy c uses porportional ratios between the clusters
# emb_df = (
#     pd.DataFrame(
#         [word_vector.wv.get_vector(str(n)) for n in word_vector.wv.key_to_index],
#         index = word_vector.wv.key_to_index
#     )
# )
# print(emb_df.shape)
# fuzzy_c = skfda.ml.clustering.FuzzyCMeans(random_state=0)
# fuzzy_c.fit(emb_df)

# %% [markdown]
# - Generate N gram, frequency, and relative frequency
unigram_sentence, unigram_frequency, unigram_relative_frequency = n_gram(cleaned_stem_text, 1)
bigram_sentence, bigram_frequency, bigram_relative_frequency = n_gram(cleaned_stem_text, 2)
trigram_sentence, trigram_frequency, trigram_relative_frequency = n_gram(cleaned_stem_text, 3)
quadgram_sentence, quadgram_frequency, quadgram_relative_frequency = n_gram(cleaned_stem_text, 4)
pentagram_sentence, pentagram_frequency, pentagram_relative_frequency = n_gram(cleaned_stem_text, 5)

# %% [markdown]
# - N gram probability matrix
# - memory constraints !!!
# - could be used to replace equation with a lookup table
# 
# column = list(unigram_frequency.index)
# prob_matrix = np.diag(unigram_frequency.to_numpy(dtype='int32'))
# Add Binomials to matrix
# print(prob_matrix)
  
# %% [markdown]
# N Gram probabilities
# - ~ 30 minutes
# $$ Unigram = P(W_{1:n})= \prod_{k=1}^n P(W_{k}) $$
# $$ Bigram = P(W_{1:n})\approx\prod_{k=1}^n P(W_{k}|W_{k-1}) $$
# $$ P(W_{n}|W_{n-1}) =  \dfrac{Count(W_{n-1}W{n})}{Count(W{n-1})} $$
# $$ Trigram = P(W_{1:n})\approx\prod_{k=1}^n P(W_{k}|W_{{k-2}, W_{k-1}}) $$
# - To improve could apply laplase Smoothing / skipgrams 
unigram_prob = unigram_probability(cleaned_stem_text, unigram_relative_frequency)
bigram_prob = bigram_probability(cleaned_stem_text, bigram_sentence, unigram_frequency, bigram_frequency)

# %% [markdown]
# - Combine ngram probability and cleaned text variations to dataframe
df_all_prob = df_all.reset_index()
df_all_prob['unigram_probability'] = unigram_prob.reset_index(drop=True)
df_all_prob['bigram_probability'] = bigram_prob
df_all_prob['cleaned_stem_text'] = cleaned_stem_text.reset_index(drop=True)
df_all_prob['cleaned_nonstem_text'] = cleaned_nonstem_text.reset_index(drop=True)
df_all_prob['bigram_sentence'] = bigram_sentence.reset_index(drop=True)
df_all_prob['trigram_sentence'] = trigram_sentence.reset_index(drop=True)
df_all_prob['quadgram_sentence'] = quadgram_sentence.reset_index(drop=True)
df_all_prob['pentagram_sentence'] = pentagram_sentence.reset_index(drop=True)
# Converting timestamp (HH:MM:SS) to Year-month-day to combine users on the same day
df_all_prob.insert(loc = 0, column = 'date', value = pd.to_datetime(df_all_prob['created_at']).apply(lambda x: x.strftime('%Y-%m-%d')))
df_all_prob.date = pd.to_datetime(df_all_prob['date'], format='%Y-%m-%d')
df_all_prob = df_all_prob.sort_values(by=['date'], ascending=False).drop(columns=['index'])
df_to_csv(df = df_all_prob, 
          folder = f'./data/merge/all_twitter_users', 
          file = f'/all_twitter_users_ngram.csv')
# %%
df_all_prob.head(2)
# %%