# %%
from pyairtable import Table
import pandas as pd
import dotenv, os

# %%
dotenv.load_dotenv()

LAG_DAYS = 3

airtoken = os.getenv('AIRTOKEN')
baseid = 'appWh9s6brrK8dFG2'
table_name_pos = 'tblY6G3aBPsC5gr6Q'
table_name_neg = 'tblR9SWSxfZK3H9Hr'
backupfile = 'airtable_backup.csv'

positive_table = Table(airtoken, baseid, table_name_pos)
negative_table = Table(airtoken, baseid, table_name_neg)

day_threshold = pd.Timestamp.today().tz_localize('UTC') - pd.Timedelta(days=LAG_DAYS)

# %%
def explode_dict(df, columns):
    for col in columns:
        df = pd.concat(
            [df.drop(col, axis=1), df[col].apply(pd.Series)], axis=1)
    return df

def load_airtable(negative_table, positive_table):
    def process_df(table, label):
        df = pd.DataFrame(table.all())
        df = explode_dict(df, ['fields'])
        df['label'] = label
        return df

    neg = process_df(negative_table, 0)
    pos = process_df(positive_table, 1)

    # concat
    airtable = pd.concat([pos, neg]).set_index('id')
    airtable['created_at'] = pd.to_datetime(airtable['created_at'])
    airtable['updated_at'] = pd.to_datetime(airtable['updated_at'])
    airtable = airtable.sort_values(by='created_at', ascending=False)
    airtable['erro'] = airtable['erro'].fillna(False)

    return airtable

def load_local(backupfile):
    if not os.path.isfile(backupfile):
        print('Backup file does not exist. Creating new backup file.')
        backup = load_airtable(negative_table, positive_table)
        backup.to_csv(backupfile)
    else:
        print('Backup file exists. Loading backup file.')
        backup = pd.read_csv(backupfile, index_col='id')
    return backup

def update_backup(backup, airtable):
    # Separate new records
    new_records = airtable[airtable.index.isin(backup.index) == False]
    
    # Identify updated records
    common_indices = airtable.index.intersection(backup.index)
    updated_records = airtable.loc[common_indices]
    backup_updated_records = backup.loc[common_indices]
    updated_records = updated_records[updated_records['updated_at'] > backup_updated_records['updated_at']]

    # Combine new and updated records
    recent = pd.concat([new_records, updated_records])
    n_rows = recent.shape[0]

    if n_rows > 0:
        print(f'Updating backup file with {n_rows} new records.')
        backup = backup[~backup.index.isin(updated_records.index)]
        backup = pd.concat([backup, recent])
        backup.to_csv(backupfile)
    else:
        print('No new records to update.')

def delete_rows(airtable):
    delete_ids_neg = airtable[(airtable.created_at < day_threshold) & (airtable.label == 0)].index
    negative_table.batch_delete(delete_ids_neg)
    print(f'Deleted {len(delete_ids_neg)} rows in negative.')
    delete_ids_pos = airtable[(airtable.created_at < day_threshold) & (airtable.label == 1)].index
    positive_table.batch_delete(delete_ids_pos)
    print(f'Deleted {len(delete_ids_pos)} rows in positive.')

# %%
if __name__ == '__main__':
    backup = load_local(backupfile)
    airtable = load_airtable(negative_table, positive_table)
    update_backup(backup, airtable)
    delete_rows(airtable)


