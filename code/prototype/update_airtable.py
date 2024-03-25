# %%
import transformers
import torch
import json
import pandas as pd
from datetime import datetime
import time
from pyairtable import Table
import os, re
import dotenv
import tweepy
from emoji import demojize
import pytz
from fuzzywuzzy import process
from urllib.parse import quote
import requests
import time
import logging
logging.getLogger().setLevel(logging.ERROR)


# %%
dotenv.load_dotenv()

# Tokens
airtoken = os.getenv('AIRTOKEN')
hf_token = os.getenv('HFTOKEN')
consumer_key = os.getenv('TWITTER_API_KEY')
consumer_secret = os.getenv('TWITTER_API_SECRET_KEY')
access_token_key = os.getenv('TWITTER_ACCESS_TOKEN')
access_token_secret = os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
bitly_token = os.getenv("BITLYTOKEN")

# Airtable
baseid = 'apprUvdn1BGp0Gx2O'
table_name_pos = 'tbltJ2OrrZHhXJjsC'
table_name_neg = 'tblmMeH9npepVa13d'
table_name_broad = 'tblqkKDMiTmLarOkF'
table_config = 'tbl1gpFXhaA9qJf5S'
positive_table = Table(airtoken, baseid, table_name_pos)
negative_table = Table(airtoken, baseid, table_name_neg)
broad_table = Table(airtoken, baseid, table_name_broad)

# Twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)
api = tweepy.API(auth)

# Query
myquery = '-from:fogocruzadorj tiro OR tiroteio OR baleado OR (disparo -(pergunto)) OR caveirao OR (aco voando) OR pipoco OR (largando dedo) OR (bala (comendo OR comeu OR voou OR cantou OR vou OR voando OR cantando OR cantar OR comer OR voar)) OR (operação (policial OR policiais))'
myquery_simple = 'lang:pt -filter:retweets -from:fogocruzadorj tiro OR tiroteio OR (aco voando) OR (bala (comendo OR comeu OR voou OR cantou OR vou OR voando OR cantando OR cantar OR comer OR voar))'
longlat = "-22.63068,-43.15247,150km"

# model
pretrainedmodel="<username>/oii-fc-v1"
tokenizer = transformers.AutoTokenizer.from_pretrained(pretrainedmodel,use_auth_token=hf_token)
model = transformers.AutoModelForSequenceClassification.from_pretrained(pretrainedmodel, num_labels=2,use_auth_token=hf_token)
    
WAIT_TIME = 3 # min
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def replace_urls(text):
    # Regular expression to match URLs
    url_pattern = re.compile(
        r'(http[s]?://|www\.)'
        r'(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    )
    
    # Replace URLs with the tag #URL
    return url_pattern.sub('URL', text)

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

def standardize_location(input_string):
    if len(input_string) > 0:
        input_string = input_string.lower()

        known_standards = {
        "rio de janeiro": "RJ",
        "rj": "RJ",
        "021": "RJ",
        "rio de janeiro, brasil": "RJ",
    }
        best_match, similarity = process.extractOne(input_string.lower(), known_standards.keys())
        if similarity >= 80:  # Set a threshold for similarity score
            standardized_value = known_standards[best_match]
            return input_string.replace(best_match, standardized_value, 1)  # Replace only the first occurrence
        else:
            return input_string
    else:
        return input_string    
    
def get_probabilities(text):
    text = cleantxt(text)
    inputs = tokenizer(text, return_tensors="pt")
    outputs = model(**inputs)

    # Softmax function to convert logits to probabilities
    probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)

    # The probability for each class
    prob_class_0 = probabilities[0][0].item()
    prob_class_1 = probabilities[0][1].item()
    return prob_class_0, prob_class_1

def generate_reply_url(tweet, text="O​​lá! Poderia informar o local e horário aproximado dos tiros? Pode ser via DM."):
    base_url = "https://twitter.com/intent/tweet"
    text = quote(text)
    url = f"{base_url}?in_reply_to={tweet['id']}&text={text}"
    return url

def generate_twlink(tweet):
    return 'https://twitter.com/' + str(tweet['user']['id']) + '/status/' + str(tweet['id'])

def get_existing_ids(table):
    records = table.all()
    ids = []
    for record in records:
        try:
            ids.append(record['fields']['tw_id'])
        except:
            pass
    return ids

def prepare_tweet(twjson):
    data = twjson
    timestamp = datetime.strptime(data["created_at"], "%a %b %d %H:%M:%S %z %Y").astimezone(pytz.timezone('America/Sao_Paulo'))
    
    record = {
        'tw_id': str(data['id']),
        'texto': data['full_text'],
        'created_at': timestamp.isoformat(),
        'dia': timestamp.strftime("%Y-%m-%d"),
        'hora': timestamp.strftime("%H:%M"),
        'usr_localizacao': standardize_location(data['user']['location']),
        'usr_bio': data['user']['description'],
        'tw_location': data['geo'],
        'url': generate_twlink(data),
        'reply_url': generate_reply_url(data)
    }
    return record

def predict_upload(data, broad=False):
    if broad:
        predproba = get_probabilities(data['full_text'])
        processed_tweet = prepare_tweet(data)
        processed_tweet['prob_class_1'] = predproba[1]
        if predproba[1] >= 0.5:
            broad_table.create(processed_tweet)
        else:
            pass
    else:
        predproba = get_probabilities(data['full_text'])
        processed_tweet = prepare_tweet(data)
        processed_tweet['prob_class_0'] = predproba[0]
        processed_tweet['prob_class_1'] = predproba[1]
        if predproba[1] >= 0.5:
            positive_table.create(processed_tweet)
        else:
            negative_table.create(processed_tweet)

def predict_upload_with_retry(twjson,broad=False):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            predict_upload(twjson,broad=broad)
            return
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            print(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            retries += 1

    # print("Maximum number of retries exceeded. Unable to complete the operation.")

def fetch_and_store_tweets(api, myquery, longlat, positive_table, negative_table,current_ids):
    last_id = os.getenv("LAST_TWEET_ID")
    
    # print("Starting new run. Current last id:", last_id)
    tw_batch = api.search_tweets(q=myquery, result_type='recent', tweet_mode='extended', geocode=longlat,\
                                 since_id=last_id, count=100)

    tw_batch.reverse()
    #print('Len of batch: ',len(tw_batch))
    new_entries = 0
    for message in tw_batch:
        twjson = message._json
        # print('Processing:',twjson['full_text'])
        if twjson['id_str'] not in current_ids:
            predict_upload_with_retry(twjson)
            last_id = twjson['id_str']
            os.environ["LAST_TWEET_ID"] = last_id
            new_entries += 1
        else:   
            # print(twjson['full_text'])
            pass
    # print("Sleeping. New entries added:", new_entries)

def fetch_and_store_tweets_broad(api, myquery, longlat, broad_table,current_ids):
    last_id = os.getenv("LAST_TWEET_ID_broad")    
    # print("Starting new run with broad query. Current last id:", last_id)
    tw_batch = api.search_tweets(q=myquery_simple, result_type='recent', tweet_mode='extended',since_id=last_id, count=100)

    tw_batch.reverse()
    # print('Len of batch: ',len(tw_batch))
    new_entries = 0
    for message in tw_batch:
        twjson = message._json
        if twjson['id_str'] not in current_ids:
            predict_upload_with_retry(twjson,broad=True)
            last_id = twjson['id_str']
            os.environ["LAST_TWEET_ID_broad"] = last_id
            # print(">> Uploaded: ",twjson['full_text'])
            new_entries += 1
        else:
            continue #print(twjson['full_text'][:25])
    # print("Sleeping. New entries added:", new_entries)


current_ids = get_existing_ids(positive_table)
current_ids.extend(get_existing_ids(negative_table))
current_ids.extend(get_existing_ids(broad_table))
fetch_and_store_tweets(api, myquery, longlat, positive_table, negative_table,current_ids)
current_ids.extend(get_existing_ids(positive_table))
fetch_and_store_tweets_broad(api, myquery, longlat, broad_table,current_ids)
