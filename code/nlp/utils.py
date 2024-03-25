import re
import pandas as pd
from sklearn.metrics import classification_report, roc_auc_score, roc_curve#, plot_roc_curve
import matplotlib.pyplot as plt
import numpy as np
import emoji

fogocruzado_id = '750331456891850752'
RANDOM_STATE_SEED = 123
np.random.seed(RANDOM_STATE_SEED)


def demojize(text):
    return emoji.demojize(text).replace(":"," ").replace("_"," ")

def replace_urls(text):
    # Regular expression to match URLs
    url_pattern = re.compile(
        r'(http[s]?://|www\.)'
        r'(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    )
    
    # Replace URLs with the tag #URL
    return url_pattern.sub('URL', text)


def explode_dict(df, columns):
    for col in columns:
        df = pd.concat(
            [df.drop(col, axis=1), df[col].apply(pd.Series)], axis=1)
    return df

def delete_tweets(df):
    df = df[df['author_id'] != fogocruzado_id]
    print('After removing Fogo Cruzado tweets:', df.shape)
    # remove duplicates
    df = df.drop_duplicates(subset=['text'])
    print('After removing duplicates texts:', df.shape)
    df = df[~df['text'].str.startswith('RT')]
    print('After removing retweets:', df.shape)
    df = df[df['in_reply_to_user_id'].isnull()]
    print('After removing replies:', df.shape)
    remove_txts = ['is temporarily unavailable','@fogocruzadoapp','fogocruzado','#FogoCruzadoRJ']

    for txt in remove_txts:
        df = df[~df['text'].str.contains(txt)]
        # df['entities.urls'] = df['entities.urls'].astype(str)
        # df = df[~df['entities.urls'].str.contains(txt)]
        print(f'After removing {txt}:', df.shape)
    
    return df

def cleantxt(text, lower=False):
    text = str(text)
    
    if lower:
        text = text.lower() # convert to lowercase
    text = replace_urls(text) # replace urls by token
    text = re.sub(r'\s+', ' ', text) # trim extra spaces
    text = re.sub(r'@\w+', 'USR', text) # remove user handles by token
    text = re.sub(r'(.)\1{3,}', r'\1\1\1\1', text) # remove repetition of char
    text = re.sub(r'[^\w\s]', ' ', text) # remove commas and other punctuation
  
    text = demojize(text)
    
    return text
