from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Schemas for nested JSON columns
def SCHEMA():
    """
    Define the schema for the  DataFrame based on TMDB API response structure.
    
    Returns:
        StructType: Spark schema for movie data
    """
    basic_fields = [
        StructField('id', IntegerType(), False),  
        StructField('title', StringType(), True),  
        StructField('tagline', StringType(), True),  
        StructField('release_date', StringType(), True),
        StructField('original_language', StringType(), True), 
        StructField('budget', LongType(), True),  
        StructField('revenue', LongType(), True), 
        StructField('vote_count', IntegerType(), True), 
        StructField('vote_average', DoubleType(), True), 
        StructField('popularity', DoubleType(), True), 
        StructField('runtime', IntegerType(), True),  
        StructField('overview', StringType(), True),  
        StructField('poster_path', StringType(), True)  
    ]
    
    collection_field = StructField('belongs_to_collection', MapType(StringType(), StringType()), True)

    # Create strucfields for structs array
    def array_struct(name, fields: List[StructField]):
        return StructField(name, ArrayType(StructType(fields)), True)
 
    array_fields = [
        array_struct('genres', [
            StructField('id', IntegerType(), True),
            StructField('name', StringType(), True)  
        ]),
        array_struct('production_companies', [
            StructField('id', IntegerType(), True), 
            StructField('name', StringType(), True)  
        ]),
        array_struct('production_countries', [
            StructField('iso_3166_1', StringType(), True), 
            StructField('name', StringType(), True)  
        ]),
        array_struct('spoken_languages', [
            StructField('iso_639_1', StringType(), True), 
            StructField('name', StringType(), True)  
        ])
    ]
    '''Credits is a struct with two keys: cast and crew of which each is an array of structs like a (lists of dictionaries)
    and each struct has fields (name, character or job)
    '''
    credits_field = StructField(
        'credits',
        StructType([
            StructField('cast', ArrayType(StructType([
                StructField('name', StringType(), True),  
                StructField('character', StringType(), True) 
            ])), True),
            StructField('crew', ArrayType(StructType([
                StructField('name', StringType(), True),  
                StructField('job', StringType(), True)  
            ])), True)
        ]),
        True
    )
    
    # Returns a StructType combining all fields for the DataFrame.
    return StructType(basic_fields + [collection_field] + array_fields + [credits_field])



def clean_data(df):
    # Drop unnecessary columns first
    cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    df = df.drop(*cols_to_drop)
    
    # Extract nested fields directly from JSON structure
    # Extract nested fields
    df = df.withColumn("collection_name", F.col("belongs_to_collection.name"))
    df = df.withColumn("genre_names", F.expr("concat_ws('|', transform(genres, x -> x.name))"))
    df = df.withColumn("spoken_languages", F.expr("concat_ws('|', transform(spoken_languages, x -> x.name))"))
    df = df.withColumn("production_countries", F.expr("concat_ws('|', transform(production_countries, x -> x.name))"))
    df = df.withColumn("production_companies", F.expr("concat_ws('|', transform(production_companies, x -> x.name))"))
    # df      .withColumn("director", F.expr("""
    #           array_join(
    #               transform(
    #                   filter(credits.crew, x -> x.job = 'Director'),
    #                   x -> x.name
    #               ),
    #               '|'
    #           )"""))
    
    df = df.withColumn("cast", col("credits.cast")) \
           .withColumn("crew", col("credits.crew")) \
           .withColumn("cast_size", size(col("cast"))) \
           .withColumn("crew_size", size(col("crew"))) \
           .withColumn("director", expr("""filter(crew, x -> x.job = 'Director')""")) \
           .withColumn("director", expr("""transform(director, x -> x.name)""")) \
           .withColumn("director", expr("""array_join(director, '|')"""))
    
    # Drop original nested columns
    df = df.drop("crew")

    df = df.withColumn("director", expr(
        """
        CASE
            WHEN size(filter(credits.crew, x->x.job = 'Director')) >0
            THEN filter(credits.crew, x->x.job = 'Director')[0].name
            ELSE NULL
        END
        """
    ))
    
    # Handle missing values and data types
    df = df.withColumn("budget", col("budget").cast(DoubleType())) \
           .withColumn("id", col("id").cast(IntegerType())) \
           .withColumn("popularity", col("popularity").cast(DoubleType())) \
           .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))
          
    # Replace zeros with null
    df = df.withColumn("budget", F.when(F.col("budget") == 0, None).otherwise(F.col("budget"))) \
           .withColumn("revenue", F.when(F.col("revenue") == 0, None).otherwise(F.col("revenue"))) \
           .withColumn("runtime", F.when(F.col("runtime") == 0, None).otherwise(F.col("runtime")))
    
    # Convert 'budget' and 'revenue' to million USD
    df = df.withColumn("budget_musd", F.col("budget") / 1000000) \
           .withColumn("revenue_musd", F.col("revenue") / 100000)

    # Handle text placeholders
    df = df.withColumn("overview", F.when(F.col("overview") != "No Data", F.col("overview")))
    df = df.withColumn("tagline", F.when(F.col("tagline") != "No Data", F.col("tagline")))
          
    # Handle vote averages
    df = df.withColumn("vote_average", F.when(F.col("vote_count") > 0, F.col("vote_average"))
          
          # Final cleanup
          .dropDuplicates(["id"])
          .dropna(subset=["id", "title"])
          .filter(F.countNonNulls("id", "title", "release_date") >= 3)
    )
    
    return df


def calculate_financial_metrics(df):
    df = (df
          .withColumn("budget_musd", F.col("budget") / 1e6)
          .withColumn("revenue_musd", F.col("revenue") / 1e6)
          .withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
          .withColumn("roi", F.col("revenue_musd") / F.col("budget_musd"))
          )
    return df

def filter_movies_by_actor_director(df, actor: str, director: str):
    return (df
            .filter(F.col("cast").rlike(f"\\b{actor}\\b") & 
                    F.col("director").rlike(f"\\b{director}\\b"))
            .orderBy("runtime")
           )

def filter_movies_by_genre_and_actor(df, genre: str, actor: str):
    return (df
            .filter(F.col("genres").rlike(f"\\b{genre}\\b") & 
                    F.col("cast").rlike(f"\\b{actor}\\b"))
            .orderBy(F.col("vote_average").desc())
           )

def plot_movie_analysis_spark(spark_df, plot_type='all'):
    """Spark-compatible version of the analysis plots."""
    
    # Helper for common operations
    def prepare_plot_data():
        return spark_df.withColumn(
            "release_year", F.year(F.to_date("release_date")))
    
    def plot_revenue_vs_budget():
        # Collect aggregated data to driver
        plot_df = spark_df.select("revenue_musd", "budget_musd").na.drop().sample(0.1).toPandas()
        
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=plot_df, x='revenue_musd', y='budget_musd')
        plt.title('Revenue vs Budget')
        plt.xlabel('Budget (million USD)')
        plt.ylabel('Revenue (million USD)')
        plt.show()

    def plot_roi_by_genre():
        # Spark-side processing
        genre_df = spark_df.withColumn(
            "genre", F.explode(F.split("genres", "|")))
        
        agg_df = genre_df.groupBy("genre").agg(
            F.mean("roi").alias("mean_roi"),
            F.count("*").alias("count")
        ).filter("count > 10").toPandas()

        plt.figure(figsize=(12, 6))
        sns.boxplot(data=agg_df, x='genre', y='mean_roi')
        plt.xticks(rotation=45)
        plt.title('ROI Distribution by Genre')
        plt.show()

    def plot_yearly_trends():
        yearly_df = prepare_plot_data().groupBy("release_year").agg(
            F.mean("budget_musd").alias("avg_budget"),
            F.mean("revenue_musd").alias("avg_revenue")
        ).na.drop().toPandas()

        yearly_df.plot(x='release_year', kind='bar', figsize=(12,6))
        plt.title('Yearly Trends')
        plt.show()

    # ... similar adaptations for other plots ...

    # Same plot selection logic as original
    plot_functions = {
        'revenue_vs_budget': plot_revenue_vs_budget,
        'roi_by_genre': plot_roi_by_genre,
        # ... other plot functions ...
    }
    
    # Same plot type handling
    # ...