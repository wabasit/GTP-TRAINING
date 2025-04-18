import json
import requests
import pandas as pd
import time
import logging

from config import API_KEY

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data():
    """
    This function extracts and saves movie data from the Movies API into a CSV file.
    It handles errors and skips movies that are not found or return errors.
    """

    movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397,
                 20818, 24428, 168259, 99861, 284054, 12445, 181808,
                 330457, 351286, 109445, 321612, 260513]

    movie_data = []
    failed_movies = []

    for movie_id in movie_ids:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}&append_to_response=credits"

        try:
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                movies = response.json()
                movie_data.append(movies)
                logging.info(f"Successfully fetched movie ID {movie_id}")
            else:
                logging.warning(f"Failed to fetch movie ID {movie_id}: Status code {response.status_code}")
                failed_movies.append(movie_id)

        except requests.exceptions.RequestException as e:
            logging.error(f"Exception occurred for movie ID {movie_id}: {e}")
            failed_movies.append(movie_id)
        
        time.sleep(0.5)

    if movie_data:
        movie_df = pd.json_normalize(movie_data)
        movie_df.to_csv('raw_movie_data.csv', index=False)
        logging.info(f"Successfully saved data for {len(movie_data)} movies.")
    else:
        logging.error("No movie data was fetched.")
        return None

    if failed_movies:
        logging.warning(f"The following movie IDs failed to fetch: {failed_movies}")