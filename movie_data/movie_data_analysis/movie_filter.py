import pandas as pd

def filter_movies_by_actor_director(df: pd.DataFrame, actor: str, director: str) -> pd.DataFrame:
    return df[
        df['cast'].str.contains(actor, case=False, na=False) &
        df['director'].str.contains(director, case=False, na=False)
    ].sort_values(by='runtime')

def filter_movies_by_genre_and_actor(df: pd.DataFrame, genre: str, actor: str) -> pd.DataFrame:
    return df[
        df['genres'].str.contains(genre, case=False, na=False) &
        df['cast'].str.contains(actor, case=False, na=False)
    ].sort_values(by='vote_average', ascending=False)

def filter_released_movies(df: pd.DataFrame) -> pd.DataFrame:
    return df[df['status'] == 'Released'].drop(columns=['status'])

def filter_valid_movies(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset='id')
    df = df.dropna(subset=['id', 'title'])
    return df[df.count(axis=1) >= 10].reset_index(drop=True)
