import requests
import time
import logging
import json
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import StructType
from utility_functions import SCHEMA, parse_movie_data
from config import API_KEY
from typing import List
import os

def fetch_apis(movie_id_list: List[int], schema: StructType) -> DataFrame:
    """Fetch movie data from TMDB API and return a Spark DataFrame."""

    log_dir = "pyspark-data-analysis/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Configure logging with proper path
    log_file = os.path.join(log_dir, "movie_logs.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    spark = SparkSession.builder.appName("Movie_Data_Analysis").getOrCreate()
    if not API_KEY:
        raise ValueError("API_KEY is not set.")

    fetched_records = []

    for movie_id in movie_id_list:
        try:
            url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}&append_to_response=credits"
            url = url.format(movie_id=movie_id, api_key=API_KEY)
            response = requests.get(url)
            response.raise_for_status()
            movie_json = response.json()

            parsed_record = parse_movie_data(movie_json)
            fetched_records.append(Row(**parsed_record))

            logging.info(f"Fetched movie: {parsed_record['title']} (ID: {parsed_record['id']})")

        except requests.exceptions.RequestException as req_err:
            logging.error(f"Network error while fetching movie ID {movie_id}: {req_err}")
        except Exception as err:
            logging.error(f"Error processing movie ID {movie_id}: {err}")

    return spark.createDataFrame(fetched_records, schema)

