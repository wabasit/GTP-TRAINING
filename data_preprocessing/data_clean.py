import pandas as pd


def clean(data):
    cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    data.drop(columns=cols_to_drop, inplace=True)

    data['genres'] = data['genres'].apply(lambda genres: '|'.join(g['name'] for g in genres)\
                                           if isinstance(genres, list) else '')
    
    data['spoken_languages'] = data['spoken_languages'].apply(lambda langs:\
                                 [lan['english_name'] for lan in langs if 'english_name' in lan])
    
    data['spoken_languages'] = data['spoken_languages'].apply(lambda langs: '|'.join(lan for lan in langs))

    data['production_countries'] = data['production_countries'].apply(lambda prod_cntry:\
                             [prod_c['name'] for prod_c in prod_cntry] if isinstance(prod_cntry, list) else '')
    
    data['production_countries'] = data['production_countries'].apply(lambda prod_cntry:\
                                     '|'.join(prod_c for prod_c in prod_cntry))
    
    data['production_companies'] = data['production_companies'].apply(lambda p_com:\
                                 [pc['name'] for pc in p_com] if isinstance(p_com, list) else '')
    
    data['production_companies'] = data['production_companies'].apply(lambda p_com: '|'.join(pc for pc in p_com))
    
    data['belongs_to_collection'] = data['belongs_to_collection'].apply(lambda x:\
                                     x['name'] if isinstance(x, dict) and 'name' in x else None)
    
    return data