# spark_streaming_to_postgres.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType
import json

# Load PostgreSQL connection config
with open('postgres_connection.json') as f:
    pg_config = json.load(f)

spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .config("spark.executor.extraJavaOptions", 
            f"-Dspark.postgresql.maxPoolSize={pg_config['jdbc']['connectionPool']['maximumPoolSize']}") \
    .getOrCreate()

# Define the schema manually for consistency
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", IntegerType()) \
    .add("product_id", StringType()) \
    .add("event_type", StringType()) \
    .add("event_time", TimestampType()) \
    .add("price", DoubleType()) \
    .add("category", StringType()) \
    .add("user_device", StringType()) \
    .add("user_location", StringType())

# Read CSV files as a stream
input_stream = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .csv("data/")  # watch the 'data' folder where CSVs are saved

# Define the function to write each batch into PostgreSQL
def write_to_postgres(batch_df, batch_id):
    (batch_df.write
     .format("jdbc")
     .option("url", pg_config['jdbc']['url'])
     .option("dbtable", "user_events")
     .option("user", pg_config['jdbc']['user'])
     .option("password", pg_config['jdbc']['password'])
     .option("driver", pg_config['jdbc']['driver'])
     .option("batchsize", pg_config['jdbc']['properties']['batchSize'])
     .option("rewriteBatchedInserts", "true")
     .option("numPartitions", 10)
     .mode("append")
     .save())

# Start the streaming query
query = input_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "checkpoint/") \
    .start()

query.awaitTermination()