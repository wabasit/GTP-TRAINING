from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType
import json
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

print("Starting spark_streaming_to_postgres.py...")

# Load PostgreSQL connection config
print("Attempting to load postgres_connection.json...")
try:
    with open('/home/jovyan/work/postgres_connection.json') as f:
        pg_config = json.load(f)
    print("postgres_connection.json loaded successfully:", pg_config)
except FileNotFoundError as e:
    print(f"Error: Could not find postgres_connection.json: {e}")
    raise
except json.JSONDecodeError as e:
    print(f"Error: Invalid JSON in postgres_connection.json: {e}")
    raise
except Exception as e:
    print(f"Error loading postgres_connection.json: {e}")
    raise

# Initialize Spark session
print("Initializing Spark session...")
try:
    spark = SparkSession.builder \
        .appName("EcommerceStreaming") \
        .config("spark.jars", "/home/jovyan/jars/postgresql-42.7.3.jar") \
        .config("spark.sql.streaming.minBatchesToRetain", "2") \
        .getOrCreate()
    print("Spark session initialized.")
except Exception as e:
    print(f"Error initializing Spark session: {e}")
    raise

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

# Read CSV files as a stream with controlled micro-batch size
print("Setting up input stream from /opt/workspace/data/...")
try:
    input_stream = spark.readStream \
        .schema(schema) \
        .option("header", True) \
        .option("maxFilesPerTrigger", 1) \
        .csv("/opt/workspace/data/")
    print("Input stream set up successfully.")
except Exception as e:
    print(f"Error setting up input stream: {e}")
    raise

# Limit records per batch (e.g., 1000 records per micro-batch)
def limit_batch_size(df, max_records=1000):
    window = Window.orderBy("event_id")
    limited_df = df.withColumn("row_num", row_number().over(window)) \
                   .filter("row_num <= {}".format(max_records)) \
                   .drop("row_num")
    return limited_df

# Write each micro-batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    try:
        batch_df = limit_batch_size(batch_df, max_records=1000)
        record_count = batch_df.count()
        if record_count > 0:
            print(f"Processing batch {batch_id} with {record_count} records")
            (batch_df.write
             .format("jdbc")
             .option("url", pg_config['jdbc']['url'])
             .option("dbtable", "user_events")
             .option("user", pg_config['jdbc']['user'])
             .option("password", pg_config['jdbc']['password'])
             .option("driver", pg_config['jdbc']['driver'])
             .option("batchsize", pg_config['jdbc']['properties'].get('batchSize', 1000))
             .option("rewriteBatchedInserts", "true")
             .option("numPartitions", 10)
             .mode("append")
             .save())
            print(f"Batch {batch_id} successfully written to PostgreSQL with {record_count} records")
        else:
            print(f"Batch {batch_id} is empty")
    except Exception as e:
        print(f"Error writing batch {batch_id} to PostgreSQL: {e}")
        raise

# Test JDBC connection before starting the stream
print("Testing JDBC connection to PostgreSQL...")
try:
    result = spark.read \
        .format("jdbc") \
        .option("url", pg_config['jdbc']['url']) \
        .option("dbtable", "user_events") \
        .option("user", pg_config['jdbc']['user']) \
        .option("password", pg_config['jdbc']['password']) \
        .option("driver", pg_config['jdbc']['driver']) \
        .load() \
        .limit(1)
    result.show()
    print("Successfully connected to PostgreSQL")
except Exception as e:
    print(f"Failed to connect to PostgreSQL: {e}")
    raise

# Start the streaming query
print("Starting streaming query...")
try:
    query = input_stream.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "/home/jovyan/work/checkpoint/") \
        .trigger(processingTime="10 seconds") \
        .start()
    print("Streaming query started.")
    query.awaitTermination()
    print("Streaming query terminated.")
except Exception as e:
    print(f"Error starting streaming query: {e}")
    raise