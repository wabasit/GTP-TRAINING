import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import json
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, when, lit, concat_ws, array, explode, to_date, udf, from_json, regexp_replace, split
)
from pyspark.sql.types import (
    StringType, ArrayType, StructType, StructField, FloatType, IntegerType, DoubleType, LongType
)

# Define SCHEMA for TMDB API response
SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("budget", LongType(), True),  # TMDB returns int
    StructField("revenue", LongType(), True),  # TMDB returns int
    StructField("genres", StringType(), True),  # JSON string
    StructField("credits", StringType(), True),  # JSON string
    StructField("release_date", StringType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("vote_average", DoubleType(), True),  # TMDB returns float
    StructField("vote_count", LongType(), True),
    StructField("popularity", DoubleType(), True),  # TMDB returns float
    StructField("status", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("poster_path", StringType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("belongs_to_collection", StringType(), True),  # JSON string
    StructField("production_companies", StringType(), True),  # JSON string
    StructField("production_countries", StringType(), True),  # JSON string
    StructField("spoken_languages", StringType(), True),  # JSON string
    StructField("original_language", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("cast", StringType(), True),
    StructField("cast_size", IntegerType(), True),
    StructField("crew_size", IntegerType(), True)
])

# Set Java networking options
os.environ["JAVA_OPTS"] = "-Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses=true"

def get_spark_session():
    return SparkSession.builder \
        .appName("MovieAnalysis") \
        .config("spark.python.worker.reuse", "false") \
            .config("spark.executor.pyspark.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.default.parallelism", "4") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
            .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
            .config("spark.network.timeout", "800s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .getOrCreate()

def parse_movie_data(movie_json):
    """Parse TMDB API response into a flat dictionary."""
    # Extract cast names for filtering (not parsed as JSON yet)
    cast = "|".join([member["name"] for member in movie_json.get("credits", {}).get("cast", [])]) if movie_json.get("credits") else ""
    
    return {
        "id": movie_json.get("id"),
        "title": movie_json.get("title"),
        "budget": movie_json.get("budget"),
        "revenue": movie_json.get("revenue"),
        "genres": json.dumps(movie_json.get("genres", [])),
        "credits": json.dumps(movie_json.get("credits", {})),
        "release_date": movie_json.get("release_date"),
        "runtime": movie_json.get("runtime"),
        "vote_average": movie_json.get("vote_average"),
        "vote_count": movie_json.get("vote_count"),
        "popularity": movie_json.get("popularity"),
        "status": movie_json.get("status"),
        "tagline": movie_json.get("tagline", "No Data"),
        "overview": movie_json.get("overview", "No Data"),
        "poster_path": movie_json.get("poster_path"),
        "backdrop_path": movie_json.get("backdrop_path"),
        "belongs_to_collection": json.dumps(movie_json.get("belongs_to_collection", {})),
        "production_companies": json.dumps(movie_json.get("production_companies", [])),
        "production_countries": json.dumps(movie_json.get("production_countries", [])),
        "spoken_languages": json.dumps(movie_json.get("spoken_languages", [])),
        "original_language": movie_json.get("original_language"),
        "origin_country": "|".join([country["iso_3166_1"] for country in movie_json.get("production_countries", [])]),
        "cast": cast,
        "cast_size": len(movie_json.get("credits", {}).get("cast", [])),
        "crew_size": len(movie_json.get("credits", {}).get("crew", []))
    }

# 1. Clean Data
def clean_data(df):
    """
    Clean the DataFrame by dropping unnecessary columns, processing JSON-like columns,
    handling missing values, and converting unrealistic values.
    """
    # Define schemas for JSON columns
    json_schemas = {
        "genres": ArrayType(
            StructType([StructField("name", StringType())])
        ),
        "production_companies": ArrayType(
            StructType([StructField("name", StringType())])
        ),
        "production_countries": ArrayType(
            StructType([StructField("name", StringType())])
        ),
        "spoken_languages": ArrayType(
            StructType([StructField("english_name", StringType())])
        ),
        "belongs_to_collection": StructType(
            [StructField("name", StringType())]
        ),
        "credits": StructType([
            StructField("cast", ArrayType(
                StructType([
                    StructField("name", StringType())
                ])
            )),
            StructField("crew", ArrayType(
                StructType([
                    StructField("job", StringType()),
                    StructField("name", StringType())
                ])
            ))
        ])
    }

    # Drop unnecessary columns if they exist
    cols_to_drop = ["adult", "imdb_id", "original_title", "video", "homepage"]
    cols_to_drop = [c for c in cols_to_drop if c in df.columns]

    # Define helper for converting zeros to null
    zero_to_null = lambda colname: when(col(colname) == 0, None).otherwise(col(colname))

    # Clean the DataFrame
    cleaned_df = (df
        # Drop unnecessary columns
        .drop(*cols_to_drop)
        
        # Parse JSON columns using defined schemas
        .select([
            from_json(col_name, json_schemas[col_name]).alias(col_name) 
            if col_name in json_schemas else col(col_name) 
            for col_name in df.columns
        ])
        
        # Extract and transform nested fields
        .withColumn(
            "belongs_to_collection",
            col("belongs_to_collection.name")
        )
        .withColumn(
            "genres",
            concat_ws("|", col("genres.name"))
        )
        .withColumn(
            "production_companies",
            concat_ws("|", col("production_companies.name"))
        )
        .withColumn(
            "production_countries",
            concat_ws("|", col("production_countries.name"))
        )
        .withColumn(
            "spoken_languages",
            concat_ws("|", col("spoken_languages.english_name"))
        )
        
        # Handle missing values: Convert column datatypes
        .withColumn("budget", col("budget").cast("double"))  # Cast to double for calculations
        .withColumn("id", col("id").cast("long"))
        .withColumn("popularity", col("popularity").cast("double"))
        .withColumn("vote_count", col("vote_count").cast("double"))
        .withColumn("vote_average", col("vote_average").cast("double"))
        .withColumn("runtime", col("runtime").cast("double"))
        .withColumn("revenue", col("revenue").cast("double"))  # Cast to double for calculations
        .withColumn("release_date", to_date(col("release_date")))
        
        # Convert unrealistic values: Replace 0 with null for numeric columns
        .withColumn("budget", zero_to_null("budget"))
        .withColumn("revenue", zero_to_null("revenue"))
        .withColumn("runtime", zero_to_null("runtime"))
        
        # Replace "No Data" with null in text columns
        .withColumn("overview", when(col("overview") == "No Data", None).otherwise(col("overview")))
        .withColumn("tagline", when(col("tagline") == "No Data", None).otherwise(col("tagline")))
        
        # Handle vote_average when vote_count is 0
        .withColumn("vote_average", when(col("vote_count") == 0, None).otherwise(col("vote_average")))
        
        # Drop duplicates and rows with null id or title
        .dropDuplicates(["id"])
        .dropna(subset=["id", "title"])
        
        # Keep rows with at least 10 non-null columns
        .withColumn(
            "non_null_count",
            sum(when(col(c).isNotNull(), 1).otherwise(0) for c in df.columns)
        )
        .filter(col("non_null_count") >= 10)
        .drop("non_null_count")
    )

    return cleaned_df

# 2. Calculate Financial Metrics
def calculate_financial_metrics(df):
    """
    Calculate financial metrics: budget_musd, revenue_musd, profit_musd, roi.
    """
    financial_df = (df
        # Convert budget and revenue to million USD
        .withColumn("budget_musd", col("budget") / 1e6)
        .withColumn("revenue_musd", col("revenue") / 1e6)
        
        # Calculate profit
        .withColumn("profit_musd", col("revenue_musd") - col("budget_musd"))
        
        # Calculate ROI
        .withColumn(
            "roi",
            when(col("budget_musd") != 0, col("revenue_musd") / col("budget_musd")).otherwise(None)
        )
    )
    return financial_df

# 3. Extract Director
def extract_director(df):
    """
    Extract director from credits column.
    """
    extracted_df = (df
        # Extract director from crew
        .withColumn(
            "director",
            F.expr("""
                filter(credits.crew, x -> x.job = 'Director')[0].name
            """)
        )
        # Handle cases where credits is null or director not found
        .withColumn("director", when(col("director").isNull(), "").otherwise(col("director")))
        # Drop credits column to avoid bloat
        .drop("credits")
    )
    return extracted_df

# 4. Rank Movies
def rank_movies(df, column, ascending=False, top_n=10, min_votes=0):
    """
    Rank movies based on a specified column.
    """
    ranked_df = (df
        # Filter based on minimum votes
        .filter(col("vote_count") >= min_votes)
        # Sort by specified column
        .orderBy(col(column).desc() if not ascending else col(column))
        # Select relevant columns
        .select("title", column, "vote_count")
        # Limit to top N
        .limit(top_n)
    )
    return ranked_df

# 5. Get Top Directors
def get_top_directors(df, top_n=10):
    """
    Aggregate director performance.
    """
    directors_df = (df
        # Group by director
        .groupBy("director")
        .agg(
            F.count("id").alias("total_movies"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.avg("vote_average").alias("mean_rating")
        )
        # Sort by total revenue
        .orderBy(col("total_revenue").desc())
        # Limit to top N
        .limit(top_n)
    )
    return directors_df

# 6. Get Top Franchises
def get_top_franchises(df, top_n=10):
    """
    Aggregate franchise performance.
    """
    franchises_df = (df
        # Filter for franchises
        .filter(col("belongs_to_collection").isNotNull())
        # Group by collection
        .groupBy("belongs_to_collection")
        .agg(
            F.count("id").alias("total_movies"),
            F.sum("budget_musd").alias("total_budget"),
            F.sum("revenue_musd").alias("total_revenue"),
            F.avg("revenue_musd").alias("mean_revenue"),
            F.avg("vote_average").alias("mean_rating")
        )
        # Sort by total revenue
        .orderBy(col("total_revenue").desc())
        # Limit to top N
        .limit(top_n)
    )
    return franchises_df

# 7. Compare Franchise vs Standalone
def compare_franchise_vs_standalone(df):
    """
    Compare franchise vs standalone movies.
    """
    comparison_df = (df
        # Add is_franchise column
        .withColumn("is_franchise", col("belongs_to_collection").isNotNull())
        # Group by is_franchise
        .groupBy("is_franchise")
        .agg(
            F.avg("revenue_musd").alias("mean_revenue"),
            F.expr("percentile_approx(roi, 0.5)").alias("median_roi"),
            F.avg("budget_musd").alias("mean_budget"),
            F.avg("popularity").alias("mean_popularity"),
            F.avg("vote_average").alias("mean_rating")
        )
    )
    return comparison_df

# 8. Filter Movies by Actor and Director
def filter_movies_by_actor_director(df, actor, director):
    """
    Filter movies by actor and director.
    """
    filtered_df = (df
        # Filter using regex with word boundaries
        .filter(
            col("cast").rlike(f"\\b{actor}\\b") & col("director").rlike(f"\\b{director}\\b")
        )
        # Sort by runtime
        .orderBy("runtime")
    )
    return filtered_df

# 9. Filter Movies by Genre and Actor
def filter_movies_by_genre_and_actor(df, genre, actor):
    """
    Filter movies by genre and actor.
    """
    filtered_df = (df
        # Filter using regex with word boundaries
        .filter(
            col("genres").rlike(f"\\b{genre}\\b") & col("cast").rlike(f"\\b{actor}\\b")
        )
        # Sort by vote_average
        .orderBy(col("vote_average").desc())
    )
    return filtered_df

# 10. Filter Released Movies
def filter_released_movies(df):
    """
    Filter for released movies and drop status column.
    """
    released_df = (df
        # Filter for released movies
        .filter(col("status") == "Released")
        # Drop status column
        .drop("status")
    )
    return released_df

# 11. Filter Valid Movies
def filter_valid_movies(df):
    """
    Filter valid movies with non-null id/title and sufficient non-null columns.
    """
    valid_df = (df
        # Drop duplicates and null id/title
        .dropDuplicates(["id"])
        .dropna(subset=["id", "title"])
        # Keep rows with at least 10 non-null columns
        .withColumn(
            "non_null_count",
            sum(when(col(c).isNotNull(), 1).otherwise(0) for c in df.columns)
        )
        .filter(col("non_null_count") >= 10)
        .drop("non_null_count")
    )
    return valid_df

# 12. Plot Movie Analysis
def plot_movie_analysis(df, plot_type="all"):
    """
    Generate movie analysis plots.
    """
    def plot_revenue_vs_budget():
        pd_df = df.select("revenue_musd", "budget_musd").toPandas()
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=pd_df, x="revenue_musd", y="budget_musd")
        plt.title("Revenue vs Budget")
        plt.xlabel("Budget (in million USD)")
        plt.ylabel("Revenue (in million USD)")
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    def plot_roi_distribution_by_genre():
        if df.filter(col("genres") != "").count() == 0:
            print("Warning: Genres column is empty. Skipping ROI by Genre plot.")
            return
        genre_df = (df
            .withColumn("genre_list", split(col("genres"), "\\|"))
            .withColumn("genre", explode(col("genre_list")))
        )
        pd_df = genre_df.select("genre", "roi").toPandas()
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=pd_df, x="genre", y="roi")
        plt.xticks(rotation=45)
        plt.title("ROI Distribution by Genre")
        plt.xlabel("Genre")
        plt.ylabel("ROI")
        plt.tight_layout()
        plt.show()

    def plot_popularity_vs_rating():
        pd_df = df.select("vote_average", "popularity").toPandas()
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=pd_df, x="vote_average", y="popularity")
        plt.title("Popularity vs Rating")
        plt.xlabel("Average Rating")
        plt.ylabel("Popularity")
        plt.tight_layout()
        plt.show()

    def plot_yearly_trends():
        yearly_df = df.withColumn("release_year", F.year(col("release_date")))
        yearly_data = (yearly_df
            .groupBy("release_year")
            .agg(
                F.avg("budget_musd").alias("budget_musd"),
                F.avg("revenue_musd").alias("revenue_musd")
            )
            .toPandas()
        )
        yearly_data = yearly_data.dropna()
        yearly_data.set_index("release_year")[["budget_musd", "revenue_musd"]].plot(
            kind="bar", figsize=(12, 6)
        )
        plt.title("Yearly Trends in Box Office Performance")
        plt.xlabel("Year")
        plt.ylabel("Million USD")
        plt.tight_layout()
        plt.show()

    def plot_franchise_vs_standalone():
        type_df = df.withColumn(
            "type",
            when(col("belongs_to_collection").isNotNull(), "Franchise").otherwise("Standalone")
        )
        summary = (type_df
            .groupBy("type")
            .agg(
                F.avg("revenue_musd").alias("revenue_musd"),
                F.avg("roi").alias("roi"),
                F.avg("budget_musd").alias("budget_musd"),
                F.avg("popularity").alias("popularity"),
                F.avg("vote_average").alias("vote_average")
            )
            .toPandas()
        )
        summary.set_index("type").plot(kind="bar", figsize=(12, 6))
        plt.title("Franchise vs Standalone Movie Performance")
        plt.ylabel("Average Metrics")
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.show()

    plot_functions = {
        "revenue_vs_budget": plot_revenue_vs_budget,
        "roi_by_genre": plot_roi_distribution_by_genre,
        "popularity_vs_rating": plot_popularity_vs_rating,
        "yearly_trends": plot_yearly_trends,
        "franchise_vs_standalone": plot_franchise_vs_standalone,
    }

    if isinstance(plot_type, str):
        if plot_type == "all":
            selected_plots = plot_functions.keys()
        else:
            selected_plots = [plot_type]
    elif isinstance(plot_type, list):
        selected_plots = plot_type
    else:
        raise ValueError("plot_type must be 'all', a string, or a list of plot names.")

    for pt in selected_plots:
        if pt in plot_functions:
            print(f"\nGenerating: {pt.replace('_', ' ').title()} Plot")
            plot_functions[pt]()
        else:
            print(f"Unknown plot type: {pt}")