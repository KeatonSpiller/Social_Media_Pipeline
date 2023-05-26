# %% [markdown]
# # Import Libraries

import os, glob, pandas as pd, numpy as np, re, texthero, collections, itertools, emoji, math
from nltk.util import ngrams,everygrams,skipgrams
from nltk.stem.snowball import SnowballStemmer
# from easynmt import EasyNMT, models
    
def dataframe_astypes():
    """_summary_
    
    cleanly access dataframe conversions
    
    Returns:
        dictionary: column names and pandas dataframe conversions
    """
    return { 'id': 'int64',
            'created_at': 'datetime64[ns, UTC]',
            'user':'object',
            'group':'object',
            'url': 'object',
            'favorite_count': 'int64',
            'retweet_count': 'int64',
            'hashtags':'object',
            'emojis': 'object',
            'emoji_text':'object',
            'usernames': 'object',
            'links': 'object',
            'text': 'object'}
    
def df_to_parquet(df, folder, file):
    """_summary_
        Save Dataframe as a parquet in a particular folder with specified file name
    Args:
        df (pandas): any pandas dataframe
        folder (string): folder location from source
        file (string): file to name parquet file
    """
    if not os.path.exists(folder):
        os.makedirs(folder)
    df.to_parquet(path= folder+file, index=False, engine='pyarrow')
    return

def batch_translate(x, model, chunk):
        """_summary_
            limit translation to (chunks) of text string length
        Args:
            x (Sentence string): Sentences of tokens seperated by spaces
        Returns:
            _type_: translated sentence of tokens seperated by spaces
        """
        sentence = ""
        size = len(x)
        start = 0
        end = chunk
        remaining = size
        while( remaining >= chunk):
            rnge = x[start:end]
            sentence += model.translate(rnge, 
                                        target_lang="en", 
                                        show_progress_bar=False, 
                                        max_length=chunk)
            remaining -= chunk
            start = end
            end += chunk
        if(size > end):
            sentence += model.translate(x[end:size], 
                                        target_lang="en", 
                                        show_progress_bar=False, 
                                        max_length=chunk)
        else:
            sentence += model.translate(x, 
                                        target_lang="en", 
                                        show_progress_bar=False, 
                                        max_length=chunk)  
        return sentence

def clean_text(s, words_to_remove):
    """_summary_
    grab all words from every text file, removing spaces and non nessesary words from stop list
    _why_
    Args:
        s (Pandas Series): Series of strings to clean
        words_to_remove (list): list of words to remove
    """
    print(f"Cleaning Text:\n")
    # normalize to lowercase
    s = s.str.lower()
    
    # remove website(html/www) username hashtag decimal extra spaces
    regex = r'http\S+|www\S+|@[\w]+|#[\w]+|[\d]+|[\s]{2,}'
    s = s.str.replace(regex, "", regex=True)
    
    # remove stop words
    s = s.str.replace(r'(?<!\w)(?:' +'|'.join(words_to_remove) + r')(?!\w)', "", regex=True)
    
    # replace emoji
    s = s.apply(lambda s: emoji.replace_emoji(s, ''))
    
    # translate to english -> opus-mt | m2m_100_1.2B | mBART50_m2m
    # opus-mt limited to sentence less than 512 -> all others less than 1024
    # pre downloaded m2m_100_1.2B as translation_model
    # model = EasyNMT(translator=models.AutoModel("data/translation_model"), 
    #                 cache_folder='data/translation_model/cached',
    #                 max_length=1024)
    # print(f"{s.str.len().max()}: Largest string before translation, index:{s.str.len().idxmax()}")
    # s = s.apply(lambda x: batch_translate(x, model, 512) )
    # print(f"{s.str.len().max()}: Largest string before translation, index:{s.str.len().idxmax()}")

    # remove punctuation and library touch up
    s = texthero.clean(s)
    
    # touch up remaining non characters and str split to remove leading/trailing spaces
    s = s.str.replace(r'[^\w\s]+', "", regex=True).str.split()
    
    # stemming similar words -> 'like' 'liked' 'liking' to stem:'lik'
    stemmer = SnowballStemmer("english")
    stemmed = s.apply(lambda x: [stemmer.stem(y) for y in x])
    
    # If I wish to filter frequency
    # filter out infrequent ( words used 1 or 2 times )
    # filter out frequent ( words used more than 1000 times?)
    print(f"Finished Cleaning Text:\n")
    return stemmed, s

def relative_probability(relative_frequency, cleaned_text):
    """_summary_
    Creating the probability of each individual tweet based on all tweets (set to 1)
    _why_
    Args:
        relative_frequency (Series): _description_
        cleaned_text (Series): _description_
        
    example
    cleaned_text =  [cat dog cat]            (length of tweet words) = 3
                    [shark cat]              (length of tweet words) = 2
                    [dog lamb]               (length of tweet words) = 2
                    
    relative_frequency = cat   : 3 / 7 = ~.43
                         shark : 1 / 7 = ~.14
                         dog   : 2 / 7 = ~.29
                         lamb  : 1 / 7 = ~.14
                                       = 1
    tweet_frequency =   [3/7 2/7 3/7]
                        [1/7 3/7]
                        [2/7 1/7]
                        
    tweet_probability = [3/7 + 2/7 + 3/7] / 3 (length of tweet words)
                        [1/7 + 3/7] / 2       (length of tweet words)
                        [2/7 + 1/7] / 2       (length of tweet words)
                        
    sum(tweet_probability) =  ~.38
                              ~.28
                              ~.21
                            = ~.88
    tweet_probability / sum(tweet_probability) = ~.38 / ~.88
                                                 ~.28 / ~.88
                                                 ~.21 / ~.88
                                                 
                                               = ~.43
                                                 ~.32
                                                 ~.24
                                               = 1     
    """
    
    tweet_frequency = [list(map(relative_frequency.get, tweet)) for tweet in cleaned_text]
    tweet_probability = [sum(tweet)/len(tweet) if(len(tweet) > 0) else 0.0 for tweet in tweet_frequency ]

    return tweet_probability / sum(tweet_probability)

def n_gram(cleaned_text, n):
    """_summary_

    Args:
        cleaned_text (Pandas Series): _description_
        n (integar): number of grams wanted
    Returns:
        grams, frequency and relative frequency Pandas Series
        ouputs csv files to to stats folder 
    """
    grams = pd.Series(cleaned_text.apply(lambda tweet: list(ngrams(tweet, n))))
    frequency = pd.Series(collections.Counter(list(itertools.chain.from_iterable(grams))))
    relative_frequency = frequency / len(frequency)
    
    folder = f'./data/transformed/stats'
    # book keeping output
    df_to_parquet(df = pd.DataFrame(grams), 
            folder = folder, 
            file = f'/{n}_grams.parquet')
    df_to_parquet(df = pd.DataFrame(frequency.reset_index()), 
            folder = folder, 
            file = f'/{n}_gram_frequency.parquet')
    df_to_parquet(df = pd.DataFrame(relative_frequency.reset_index()), 
            folder = folder, 
            file = f'/{n}_gram_relative_frequency.parquet')
    
    return grams, frequency, relative_frequency

# %% [markdown]
# $$ P(W_{1:n})\approx\prod_{k=1}^n P(W_{k}|W_{k-1}) $$
# $$ P(W_{n}|W_{n-1}) =  \dfrac{C(W_{n-1}W{n})}{C(W{n-1})} $$
def bigram_probability(cleaned_text, bigram_sentence, unigram_frequency, bigram_frequency):
    """_summary_
    Creating the probability of each individual tweet based on all tweets (set to 1)
    _why_
    Args:
        bigram_sentence (Series): _description_
        unigram_frequency (Series): _description_
        bigram_frequency (Series): _description_
        cleaned_text (Series): _description_
    ex.  
    P(<s> i want english food </s>)
    = P(i|<s>)P(want|i)P(english|want)P(food|english)P(</s>|food)
    
    # count of i followed by want divided by all i
    P(want|i) = Count(i|want)/ Count(i) 
    # words beginning with i divided by count of sentences
    P(i|<s>) = Count(<s>|i)/ Count(<s>) 
    # words ending with food divided by count of sentences
    P(<s>|food) = Count(<s>|food)/ Count(<s>) 
    """
    # acount for beginning and ending values P(word|<s>) (</s>|word)
    beginning_words, ending_words = cleaned_text.str[0], cleaned_text.str[-1]
    beginning_words_dict, ending_words_dict= beginning_words.value_counts(), ending_words.value_counts()

    bigram_prob = []
    for i, sentence in enumerate(bigram_sentence):
        if sentence:
            begin_prob = (beginning_words_dict[beginning_words[i:i+1]]/len(cleaned_text)).to_numpy()
            end_prob = (ending_words_dict[ending_words[i:i+1]]/len(cleaned_text)).to_numpy()
            bigram_prob.append(np.prod(np.append([bigram_frequency[tup]/unigram_frequency[tup[0]] for tup in sentence], (begin_prob * end_prob) )))
        else:
            bigram_prob.append(0)
            
    return bigram_prob

# Probabilities of N gram twitter words spoken compared to other tweets
def unigram_probability(cleaned_text, unigram_relative_frequency):
    """_summary_
    Creating the probability of each individual tweet based on all tweets (set to 1)
    _why_
    Args:
        relative_frequency (Series): _description_
        cleaned_text (Series): _description_
        
    P(students are from vallore)
    Bigram = P(are|students)*P(from|are)*P(vallore|from)
    P(are|students) = count(students|are)/count(students)
    """
    total_probability = cleaned_text.apply(lambda tweet: np.prod(list(map(unigram_relative_frequency.get, tweet))))
   
    return total_probability