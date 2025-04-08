import pandas as pd
import numpy as np

from data_preprocessing.data_clean import clean

data = clean()

def handle_missing_values(data):
    col_convert = ['budget', 'id', 'popularity']
    converted_to_num = [pd.to_numeric(data[d]) for d in col_convert]
    data['release_date'] = pd.to_datetime(data['release_date'])

def convert_values(data):
    unrealistic_val = ['budget', 'revenue', 'runtime']
    million_dollars = ['budget', 'revenue']
    placeholder_replace = ['overview', 'tagline']
    data[unrealistic_val] = data[unrealistic_val].replace(0, np.nan)
    data[million_dollars] = data[million_dollars] / 1000000
    data[placeholder_replace] = data[placeholder_replace].replace('No Data', np.nan)
    data = data.drop_duplicates()

    
