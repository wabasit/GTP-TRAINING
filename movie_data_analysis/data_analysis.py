import pandas as pd
import numpy as np

def calculate_profit(data):
    data['profit_musd'] = data['revenue_musd'] - data['budget_musd']
    return data

def calculate_roi(data):
    data['roi'] = data['revenue_musd'] / data['budget_musd']
    data.loc[data['budget_musd'] == 0, 'roi'] = np.nan
    return data

def rank_movies(data, column, ascending=False, top_n=10, min_votes=0):
    filtered = data[data['vote_count'] >= min_votes]
    ranked = filtered.sort_values(by=column, ascending=ascending).head(top_n)
    return ranked[['title', column, 'vote_count']]

def get_top_directors(data, top_n=10):
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


