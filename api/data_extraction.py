import json
import sys

import requests
import pandas as pd

from config import TOKEN, API_KEY
    
def extract_data(self, save_file_path):
    """
    This function extracts, and dump movie data from the Movies API and dump into a json file.
    Some specific movie ids were specified for extraction.
    """

    movie_ids = [299534, 19995, 140607, 299536, 597, 135397,\
                    20818, 24428, 168259, 99861, 284054, 12445,181808,\
                    330457, 351286, 109445, 321612, 260513]
    
    movie_data = []
    
    for movie_id in movie_ids:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}&append_to_response=credits"

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {TOKEN}"
        }

        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            movies = response.json()
            movie_data.append(movies)
        else:
            return f'Error Code: {response.status_code};\nMsg: Movie with {movie_id} was not found!'
        movie_df = pd.DataFrame(movie_data)

        with open(save_file_path, 'w') as f:
            data = json.dump(movie_df, f, ensure_ascii=False, indent=4)

    return data












