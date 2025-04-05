
import requests
import pandas as pd
from config import TOKEN

        
def extract_data(self):

    movie_ids = [299534, 19995, 140607, 299536, 597, 135397,\
                    20818, 24428, 168259, 99861, 284054, 12445,181808,\
                    330457, 351286, 109445, 321612, 260513]
    
    for movie_id in movie_ids:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}"

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {TOKEN}"
        }

        response = requests.get(url, headers=headers)
        data = response.json()

        df = pd.json_normalize(data)
        return df












