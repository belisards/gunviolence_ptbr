import json
import os
import pandas as pd
from tqdm import tqdm
import shutil
import polars as pl
from utils import *
from multiprocessing import Pool, cpu_count

data_folder = '../broad_search/'
final_folder = '../final_database/broad_search/'
final_unlabel = '../final_database/unlabel.parquet'

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

        del merged_df

    data['url'] = 'https://twitter.com/' + data['user_username'] + '/status/' + data['id']
    data['filename'] = file_name
    data = fix_missing_cols(data)
    return data

def process_folder(folder):
    file_list = []
    files = os.listdir(folder)
    files = [os.path.join(folder, f) for f in files if f.endswith('.json')]
    for f in files:
        temp = process_twitter_data_from_file(f)
        if temp is not None:
            file_list.append(temp)
            del temp
    if file_list:
        posts = pd.concat(file_list, ignore_index=True)
        csv_file_path = os.path.join(folder, 'result.csv')
        posts.to_csv(csv_file_path)
    else:
        pd.DataFrame().to_csv(os.path.join(folder, 'result.csv'),escapechar='\\-\\')

def process_dir(folder):
    try:
        process_folder(folder)
    except Exception as e:
        print('Error in folder:', folder, 'Error:', str(e))

def process_unlabel_parallel(folder_prefix):
    folders = os.listdir(folder_prefix)
    folders = [os.path.join(folder_prefix, f) for f in folders if os.path.isdir(os.path.join(folder_prefix, f))]
    num_cpus = max(1, cpu_count() / 2)
    print('Using', num_cpus, 'CPUs')
    with Pool(int(num_cpus)) as p:
        list(tqdm(p.imap(process_dir, folders), total=len(folders)))

def move_files(folder_prefix, path_destination):
    # ensure the destination folder exists
    os.makedirs(path_destination, exist_ok=True)

    # iterate through the folder_prefix and its subdirectories
    for root, dirs, files in os.walk(folder_prefix):
        for file in files:
            # check if file is a feather file
            if file.endswith('.csv'):
                # construct full file path
                file_path = os.path.join(root, file)
                # modify file name to include folder name to ensure uniqueness
                folder_name = os.path.basename(root)
                unique_file_name = f"{folder_name}_{file}"
                # construct destination path
                destination_path = os.path.join(path_destination, unique_file_name)
                # move the file
                shutil.move(file_path, destination_path)
    print('All files moved to:', path_destination)

def combine_files(path_destination, output_file):
    # Get the list of all CSV files in the destination directory
    files = [f for f in os.listdir(path_destination) if f.endswith('.csv')]
    print(len(files))
    
    # Initialize an empty list to hold DataFrames
    df_list = []
    
    for file in tqdm(files, desc="Merging Files"):
        # Construct the full file path
        file_path = os.path.join(path_destination, file)
        
        # Load the CSV file
        df = pl.read_csv(file_path,infer_schema_length=0)
        
        # Append the DataFrame to the list
        df_list.append(df)

    # Concatenate all DataFrames in the list into one DataFrame
    combined_df = pl.concat(df_list)

    # export polar dataframe 
    combined_df.write_parquet(output_file)

    print('All files combined into:', output_file)

def delete_files(folder_prefix):
    # iterate through the folder_prefix and its subdirectories
    for root, dirs, files in os.walk(folder_prefix):
        for file in files:
            # check if file is a csv file
            if file.endswith('.csv'):
                # construct full file path
                file_path = os.path.join(root, file)
                # delete the file
                os.remove(file_path)
    print('All files deleted from:', folder_prefix)

process_unlabel_parallel(data_folder)

print("CSVs generated")

move_files(data_folder, '../final_database/broad_search/')

print("CSVs moved")

combine_files(final_folder, final_unlabel)

print("Final files generated")

delete_files(final_folder)

print("Raw data deleted")
