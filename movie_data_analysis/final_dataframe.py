import json
import pandas as pd

from movie_data_analysis import data_clean
from movie_data_analysis.data_extraction import extract_data
from movie_data_analysis.data_analysis import get_top_directors, get_top_franchises, rank_movies, calculate_roi
from movie_data_analysis.visualization import (plot_franchise_vs_standalone, plot_popularity_vs_rating,
plot_revenue_vs_budget, plot_roi_distribution_by_genre, plot_yearly_trends
)

def generate_report():
    """
    This function implements all functions by automating them. It fetches the data, clean, analyze and visualize
    the data.
    The clean data is then saved as a csv file 
    """
    print("Fetching movie data...")
    # data = extract_data()
    data = pd.read_csv('raw_movie_data.csv')

    print("Cleaning movie data...")
    df_cleaned = data_clean.clean(data)

    print("Generating KPIs and Analysis...")
    roi_df = calculate_roi(df_cleaned)
    top_revenue = rank_movies(df_cleaned, 'revenue_musd')
    top_profit = rank_movies(df_cleaned, 'profit_musd')
    top_rated = rank_movies(df_cleaned[df_cleaned['vote_count'] >= 10], 'vote_average')
    top_directors = get_top_directors(df_cleaned)
    top_franchises = get_top_franchises(df_cleaned)

    print("Creating Visualizations...")
    plot_revenue_vs_budget(df_cleaned)
    plot_roi_distribution_by_genre(df_cleaned)
    plot_popularity_vs_rating(df_cleaned)
    plot_yearly_trends(df_cleaned)
    plot_franchise_vs_standalone(df_cleaned)

    print("Saving Final Cleaned Dataset...")
    df_cleaned.to_csv('cleaned_movie_data.csv', index=False)

    print("Report generation complete.")
    return {
        "roi_df": roi_df,
        "top_revenue": top_revenue,
        "top_profit": top_profit,
        "top_rated": top_rated,
        "top_directors": top_directors,
        "top_franchises": top_franchises
    }

generate_report()