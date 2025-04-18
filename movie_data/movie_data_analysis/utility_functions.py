import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

"""
Function to clean the extracted data.
"""
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

"""
Function to handle missing values
"""
def handle_missing_values(data):
    col_convert = ['budget', 'id', 'popularity']
    data[col_convert] = [pd.to_numeric(data[d]) for d in col_convert]
    data['release_date'] = pd.to_datetime(data['release_date'])

"""
Function to convert unrealistic values to nan
"""
def convert_values(data):
    unrealistic_val = ['budget', 'revenue', 'runtime']
    placeholder_replace = ['overview', 'tagline']
    data[unrealistic_val] = [data[d].replace(0, np.nan) for d in unrealistic_val]
    data[placeholder_replace] = [data[d].replace('No Data', np.nan) for d in placeholder_replace]
    data.loc[data['vote_counts']==0, 'vote_average'] = np.nan
    data['Released'] = data.loc[data['status']=='Released', 'status']
    data.drop(columns='status', axis=1, inplace=True)

    data['budget'] = data['budget'].replace(0, np.nan)
    data['revenue'] = data['revenue'].replace(0, np.nan)
    data['runtime'] = data['runtime'].replace(0, np.nan)

    # data['budget_musd'] = data['budget'] / 1e6
    # data['revenue_musd'] = data['revenue'] / 1e6

    data.loc[data['vote_count'] == 0, 'vote_average'] = np.nan

    for text_col in ['overview', 'tagline']:
        data[text_col] = data[text_col].replace('No Data', np.nan)

    data.drop_duplicates(inplace=True)
    data.dropna(subset=['id', 'title'], inplace=True)
    data = data[data.notna().sum(axis=1) >= 10]

    return data

def calculate_profit(data):
    data['profit_musd'] = data['revenue_musd'] - data['budget_musd']
    return data['profit_musd']

def calculate_roi(data):
    data['budget_musd'] = data['budget'] / 1000000
    data['revenue_musd'] = data['revenue'] / 1000000
    data['roi'] = data['revenue_musd'] / data['budget_musd']
    data.loc[data['budget_musd'] == 0, 'roi'] = np.nan
    return data['roi']

def rank_movies(data, column, ascending=False, top_n=10, min_votes=0):
    data['profit_musd'] = data['revenue_musd'] - data['budget_musd']
    filtered = data[data['vote_count'] >= min_votes]
    ranked = filtered.sort_values(by=column, ascending=ascending).head(top_n)
    return ranked[['title', column, 'vote_count']]

def get_top_directors(data, top_n=10):
    data['director'] = data['credits'].apply(lambda x: [i['job'] for i in x] if isinstance(x, list) else '')
    grouped = data.groupby('director').agg(
        total_movies=('id', 'count'),
        total_revenue=('revenue_musd', 'sum'),
        mean_rating=('vote_average', 'mean')
    ).sort_values(by='total_revenue', ascending=False).head(top_n)
    return grouped.reset_index()

def get_top_franchises(data, top_n=10):
    franchises = data[data['belongs_to_collection'].notna()]
    grouped = franchises.groupby('belongs_to_collection').agg(
        total_movies=('id', 'count'),
        total_budget=('budget_musd', 'sum'),
        total_revenue=('revenue_musd', 'sum'),
        mean_revenue=('revenue_musd', 'mean'),
        mean_rating=('vote_average', 'mean')
    ).sort_values(by='total_revenue', ascending=False).head(top_n)
    return grouped.reset_index()

def compare_franchise_vs_standalone(data ):
    data['is_franchise'] = data['belongs_to_collection'].notna()
    grouped = data.groupby('is_franchise').agg(
        mean_revenue=('revenue_musd', 'mean'),
        median_roi=('roi', 'median'),
        mean_budget=('budget_musd', 'mean'),
        mean_popularity=('popularity', 'mean'),
        mean_rating=('vote_average', 'mean')
    )
    return grouped.reset_index()

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


def plot_movie_analysis(df: pd.DataFrame, plot_type='all'):
    """
    Generates different movie analysis plots based on the selected plot_type.
    
    Parameters:
    - df (pd.DataFrame): The movie data.
    - plot_type (str or list): One or more plot types to display. Options:
        'revenue_vs_budget', 'roi_by_genre', 'popularity_vs_rating',
        'yearly_trends', 'franchise_vs_standalone', or 'all'.
    """
    
    def plot_revenue_vs_budget():
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df, x='revenue_musd', y='budget_musd')
        plt.title('Revenue vs Budget')
        plt.xlabel('Budget (in million USD)')
        plt.ylabel('Revenue (in million USD)')
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    def plot_roi_distribution_by_genre():
        genre_df = df.copy()
        genre_df['genre_list'] = genre_df['genres'].str.split('|')
        genre_df = genre_df.explode('genre_list')
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=genre_df, x='genre_list', y='roi')
        plt.xticks(rotation=45)
        plt.title('ROI Distribution by Genre')
        plt.xlabel('Genre')
        plt.ylabel('ROI')
        plt.tight_layout()
        plt.show()

    def plot_popularity_vs_rating():
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df, x='vote_average', y='popularity')
        plt.title('Popularity vs Rating')
        plt.xlabel('Average Rating')
        plt.ylabel('Popularity')
        plt.tight_layout()
        plt.show()

    def plot_yearly_trends():
        yearly_df = df.copy()
        yearly_df['release_date'] = pd.to_datetime(yearly_df['release_date'], errors='coerce')
        yearly_df['release_year'] = yearly_df['release_date'].dt.year
        yearly_data = yearly_df.groupby('release_year')[['budget_musd', 'revenue_musd']].mean().dropna()
        yearly_data.plot(kind='bar', figsize=(12, 6))
        plt.title('Yearly Trends in Box Office Performance')
        plt.xlabel('Year')
        plt.ylabel('Million USD')
        plt.tight_layout()
        plt.show()

    def plot_franchise_vs_standalone():
        type_df = df.copy()
        type_df['type'] = type_df['belongs_to_collection'].apply(lambda x: 'Franchise' if pd.notnull(x) else 'Standalone')
        summary = type_df.groupby('type')[['revenue_musd', 'roi', 'budget_musd', 'popularity', 'vote_average']].mean()
        summary.plot(kind='bar', figsize=(12, 6))
        plt.title('Franchise vs Standalone Movie Performance')
        plt.ylabel('Average Metrics')
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.show()

    # Map plot names to functions
    plot_functions = {
        'revenue_vs_budget': plot_revenue_vs_budget,
        'roi_by_genre': plot_roi_distribution_by_genre,
        'popularity_vs_rating': plot_popularity_vs_rating,
        'yearly_trends': plot_yearly_trends,
        'franchise_vs_standalone': plot_franchise_vs_standalone,
    }

    # Normalize input
    if isinstance(plot_type, str):
        if plot_type == 'all':
            selected_plots = plot_functions.keys()
        else:
            selected_plots = [plot_type]
    elif isinstance(plot_type, list):
        selected_plots = plot_type
    else:
        raise ValueError("plot_type must be 'all', a string, or a list of plot names.")

    # Run selected plots
    for pt in selected_plots:
        if pt in plot_functions:
            print(f"\n Generating: {pt.replace('_', ' ').title()} Plot")
            plot_functions[pt]()
        else:
            print(f"Unknown plot type: {pt}")
