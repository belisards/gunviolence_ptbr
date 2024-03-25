import pandas as pd
import json
import os
import tqdm
from utils import *

data_folder = '../fogocruzadorj_replies/'
finalfilename = '../final_database/positive.parquet'

def process_twitter_data_from_file(file_name):
    # Read JSON file
    with open(file_name, 'r') as f:
        content = f.read()
        separator = '}{'
        # if the file is empty return None
        if content == '':
            return None
        if separator in content:
            # check if separator is not in a new line, i.e. the string occurs as part of the text
            if separator not in content.splitlines():
                data = json.loads(content)
            else:
            # split two json objects into separate strings
                # print(file_name)
                content = content.split(separator)
                part1 = json.loads(content[0] + '}')
                part2 = json.loads('{' + content[1])
                # stack two json objects into one dictionary
                data = {**part1, **part2}
        else:
            data = json.loads(content)
        
    if 'data' not in data:
        return None
        
    elif 'includes' not in data:
        data = pd.DataFrame(pd.json_normalize(data['data']))
    
    # tweets with user data
    else:
        includes_users = pd.json_normalize(data).iloc[0]['includes.users']
        users = pd.DataFrame(includes_users)

        # Read tweets
        tweets = pd.DataFrame(pd.json_normalize(data['data']))

        # Add prefix to user columns
        users = users.add_prefix('user_')

        # Merge tweets and users DataFrames
        merged_df = pd.merge(tweets, users, left_on='author_id',
                            right_on='user_id', how='left')

        data = explode_dict(merged_df, ['user_public_metrics'])

    data['url'] = 'https://twitter.com/' + data['user_username'] + '/status/' + data['id']
    
    return data


def process_folder(folder):
    # print json files
    files = os.listdir(folder)
    files = [f for f in files if f.endswith('.json')]
    # get tha name of the file that has testimonial in the name 
    testimonial = [f for f in files if 'testimonial' in f]
    if len(testimonial) == 0:
        return None
    testimonial = process_twitter_data_from_file(folder + testimonial[0])
    if testimonial is None:
        return None

    posts = [f for f in files if 'fogocruzado' in f]
    posts = process_twitter_data_from_file(folder + posts[0])

     # remove posts in threads made by the same user
    
    posts = posts[posts['in_reply_to_user_id'] != fogocruzado_id]
        
    # print(f'Number of posts: {len(posts)}')
    # print(f'Number of testimonial: {len(testimonial)}')
    
    # testimonials in posts['conversation_id']
    # try:
    #     testimonial = testimonial[testimonial['conversation_id'].isin(posts['conversation_id'])]
    # except:
    #     print(folder)
    #     pass

    return testimonial

def process_positive(folder_prefix):
    # list only folders
    folders = os.listdir(folder_prefix)
    folders = [f for f in folders if os.path.isdir(folder_prefix + f)]
    df = pd.DataFrame()
    for folder in tqdm.tqdm(folders):
        folder = folder_prefix + folder + '/'
        temp = process_folder(folder)
        df = pd.concat([df, temp])
    
    return df

pos = process_positive(data_folder)

# pos = pos[cols]

pos['label'] = 1

pos.to_parquet(finalfilename, index=False)