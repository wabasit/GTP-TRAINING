import pandas as pd


def clean(data):
    cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    data.drop(columns=cols_to_drop, inplace=True)

    def split_column_vals(col):
        return col.apply(lambda x: '|'.join([i['name'] for i in x]) if isinstance(x, list) else '')
    
    def extract_names(col):
        return col.apply(lambda x: [i['name'] for i in x] if isinstance(x, list) else '')

    data['genres'] = split_column_vals(data['genres'])
    data['spoken_languages'] = split_column_vals(data['spoken_languages'])
    data['production_companies'] = split_column_vals(data['production_companies'])
    
    
    data['production_companies'] = extract_names(data['production_companies'])
    data['production_countries'] = extract_names(data['production_countries'])
    
    data['spoken_languages'] = data['spoken_languages'].apply(lambda langs:\
                                 [lan['english_name'] for lan in langs if 'english_name' in lan])
    
    data['production_countries'] = data['production_countries'].apply(lambda prod_cntry:\
                                     '|'.join(prod_c for prod_c in prod_cntry))
    
    data['belongs_to_collection'] = data['belongs_to_collection'].apply(lambda x:\
                                     x['name'] if isinstance(x, dict) and 'name' in x else None)
    
    return data