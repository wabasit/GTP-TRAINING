import requests
import time
import logging
import pandas as pd

from config import API_KEY

def fetch_api():
    """
    This function extracts and saves movie data from the Movies API into a CSV file.
    It handles errors and skips movies that are not found or return errors.
    """
    movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397,
                 20818, 24428, 168259, 99861, 284054, 12445, 181808,
                 330457, 351286, 109445, 321612, 260513]
    movie_data = []
    failed_movies = []
    
    # Configuring logging
    logging.basicConfig(filename='pyspark-data-analysis/movie_logs.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    
    for movie_id in movie_ids:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}&append_to_response=credits"
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            movies = response.json()
            movie_data.append(movies)
            logging.info(f"Movie '{movies.get('title', 'Unknown')}' (ID: {movie_id}) fetched successfully")
        except requests.exceptions.RequestException as e:
            failed_movies.append(movie_id)
            logging.error(f"Failed to fetch movie ID {movie_id}: {e}")
        time.sleep(0.5)

    if movie_data:
        try:
            df = pd.json_normalize(movie_data)
            df.to_csv('pyspark-data-analysis/movie_data.csv', index=False)
            logging.info(f"Saved {len(df)} movies to movie_data.csv")
            return df
        except Exception as e:
            logging.error(f"Failed to save movie_data.csv: {e}")
            return None
    else:
        logging.error(f"No movies fetched. Failed IDs: {failed_movies}")
        return None

if __name__ == "__main__":
    fetch_api()