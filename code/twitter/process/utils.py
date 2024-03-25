import re
import pandas as pd
import numpy as np
import emoji

cols = ['id','text','created_at','url','author_id','in_reply_to_user_id','user_location','geo.place_id']
fogocruzado_id = '750331456891850752'
RANDOM_STATE_SEED = 123
np.random.seed(RANDOM_STATE_SEED)

def fix_missing_cols(data, columns=cols):
    missing_columns = set(columns) - set(data.columns)
    for col in missing_columns:
        data[col] = None
    return data[columns]

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
    print('Before removing replies:', df.shape)
    df = df[df['in_reply_to_user_id'].isnull()]
    print('After removing replies:', df.shape)
    remove_txts = ['is temporarily unavailable','@fogocruzadoapp','fogocruzado','#FogoCruzadoRJ']

    for txt in remove_txts:
        df = df[~df['text'].str.contains(txt)]
        df['entities.urls'] = df['entities.urls'].astype(str)
        df = df[~df['entities.urls'].str.contains(txt)]
        print(f'After removing {txt}:', df.shape)
    
    return df


def get_url(df,id):
    return df[df.id == id]['url'].values[0]


