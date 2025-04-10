import pandas as pd
import numpy as np


def handle_missing_values(data):
    col_convert = ['budget', 'id', 'popularity']
    data[col_convert] = [pd.to_numeric(data[d]) for d in col_convert]
    data['release_date'] = pd.to_datetime(data['release_date'])

def convert_values(data):
    unrealistic_val = ['budget', 'revenue', 'runtime']
    placeholder_replace = ['overview', 'tagline']
    data[unrealistic_val] = [data[d].replace(0, np.nan) for d in unrealistic_val]
    data['budget_musd'] = data['budget'] / 1000000
    data['revenue_musd'] = data['revenue'] / 1000000
    data[placeholder_replace] = [data[d].replace('No Data', np.nan) for d in placeholder_replace]
    data.loc[data['vote_counts']==0, 'vote_average'] = np.nan
    data['Released'] = data.loc[data['status']=='Released', 'status']
    data.drop(columns='status', axis=1, inplace=True)

    data['budget'] = data['budget'].replace(0, np.nan)
    data['revenue'] = data['revenue'].replace(0, np.nan)
    data['runtime'] = data['runtime'].replace(0, np.nan)

    data['budget_musd'] = data['budget'] / 1e6
    data['revenue_musd'] = data['revenue'] / 1e6

    data.loc[data['vote_count'] == 0, 'vote_average'] = np.nan

    for text_col in ['overview', 'tagline']:
        data[text_col] = data[text_col].replace('No Data', np.nan)

    data.drop_duplicates(inplace=True)
    data.dropna(subset=['id', 'title'], inplace=True)
    data = data[data.notna().sum(axis=1) >= 10]

    return data


    
