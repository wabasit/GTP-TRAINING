import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_revenue_vs_budget(df: pd.DataFrame):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='revenue_musd', y='budget_musd')
    plt.title('Revenue vs Budget')
    plt.xlabel('Budget (in million USD)')
    plt.ylabel('Revenue (in million USD)')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def plot_roi_distribution_by_genre(df: pd.DataFrame):
    df = df.copy()
    df['genre_list'] = df['genres'].str.split('|')
    df = df.explode('genre_list')
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x='genre_list', y='roi')
    plt.xticks(rotation=45)
    plt.title('ROI Distribution by Genre')
    plt.xlabel('Genre')
    plt.ylabel('ROI')
    plt.show()

def plot_popularity_vs_rating(df: pd.DataFrame):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='vote_average', y='popularity')
    plt.title('Popularity vs Rating')
    plt.xlabel('Average Rating')
    plt.ylabel('Popularity')
    plt.show()

def plot_yearly_trends(df: pd.DataFrame):
    df = df.copy()
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df['release_year'] = df['release_date'].dt.year
    yearly_data = df.groupby('release_year')[['budget_musd', 'revenue_musd']].mean().dropna()
    plt.figure(figsize=(12, 6))
    yearly_data.plot(kind='bar')
    plt.title('Yearly Trends in Box Office Performance')
    plt.xlabel('Year')
    plt.ylabel('Million USD')
    plt.show()


def plot_franchise_vs_standalone(df: pd.DataFrame):
    df = df.copy()
    df['type'] = df['belongs_to_collection'].apply(lambda x: 'Franchise' if pd.notnull(x) else 'Standalone')
    summary = df.groupby('type')[['revenue_musd', 'roi', 'budget_musd', 'popularity', 'vote_average']].mean()
    summary.plot(kind='bar', figsize=(12, 6))
    plt.title('Franchise vs Standalone Movie Performance')
    plt.ylabel('Average Metrics')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()