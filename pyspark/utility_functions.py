from pyspark.sql.functions import col, when, udf, split, explode, regexp_replace, to_date, mean
from pyspark.sql.types import StringType, FloatType, ArrayType, StructType, StructField
from pyspark.sql.functions import size, array, lit
from pyspark.sql.functions import expr

# Example: extract list of names from genres column
@udf(returnType=StringType())
def extract_names_udf(val):
    try:
        return '|'.join([i['name'] for i in val])
    except:
        return ''

def clean_data(df):
    cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    for colname in cols_to_drop:
        df = df.drop(colname)

    df = df.withColumn("genres", extract_names_udf(col("genres")))
    df = df.withColumn("spoken_languages", extract_names_udf(col("spoken_languages")))
    df = df.withColumn("production_companies", extract_names_udf(col("production_companies")))
    df = df.withColumn("production_countries", extract_names_udf(col("production_countries")))

    @udf(StringType())
    def collection_name_udf(val):
        if isinstance(val, dict) and 'name' in val:
            return val['name']
        return None
    
    df = df.withColumn("belongs_to_collection", collection_name_udf(col("belongs_to_collection")))
    return df

def handle_missing_values(df):
    df = df.withColumn("budget", col("budget").cast("double")) \
           .withColumn("id", col("id").cast("long")) \
           .withColumn("popularity", col("popularity").cast("double")) \
           .withColumn("release_date", to_date("release_date"))
    return df

def convert_unrealistic_values(df):
    zero_to_null = lambda colname: when(col(colname) == 0, None).otherwise(col(colname))
    df = df.withColumn("budget", zero_to_null("budget")) \
           .withColumn("revenue", zero_to_null("revenue")) \
           .withColumn("runtime", zero_to_null("runtime"))

    for text_col in ['overview', 'tagline']:
        df = df.withColumn(text_col, when(col(text_col) == "No Data", None).otherwise(col(text_col)))

    df = df.filter(col("id").isNotNull() & col("title").isNotNull())
    return df.dropDuplicates(["id"])

def calculate_financial_metrics(df):
    df = df.withColumn("budget_musd", col("budget") / 1e6)
    df = df.withColumn("revenue_musd", col("revenue") / 1e6)
    df = df.withColumn("profit_musd", col("revenue_musd") - col("budget_musd"))
    df = df.withColumn("roi", col("revenue_musd") / col("budget_musd"))
    df = df.withColumn("roi", when(col("budget_musd") == 0, None).otherwise(col("roi")))
    return df

def filter_movies_by_actor_director(df, actor: str, director: str):
    return df.filter(
        col("cast").rlike(actor) & col("director").rlike(director)
    ).orderBy("runtime")



def filter_movies_by_genre_and_actor(df, genre: str, actor: str):
    return df.filter(
        col("genres").rlike(genre) & col("cast").rlike(actor)
    ).orderBy(col("vote_average").desc())

def filter_released_movies(df):
    return df.filter(col("status") == "Released").drop("status")

