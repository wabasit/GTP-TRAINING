import pandas as pd

from api.data_extraction import extract_data

data = extract_data()


def clean(data):
    cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    data.drop(columns=cols_to_drop, inplace=True)

    data['genres'] = data['genres'].apply(lambda genres: '|'.join(g['name'] for g in genres)\
                                           if isinstance(genres, list) else '')
    
    data['spoken_languages'].apply(lambda langs: '|'.join(l['english_name'] for l in langs)\
                                    if isinstance(langs, list) else '')

    data['production_countries'].apply(lambda countries: '|'.join(c['name'] for c in countries)\
                                        if isinstance(countries, list) else '')