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


