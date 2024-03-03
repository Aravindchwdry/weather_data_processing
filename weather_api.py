from db_config import get_redis_connection
import requests
import json

class WeatherDataFetcher:
    """
    A class to fetch weather data from an API and store it in Redis.

    Attributes:
    - url (str): The URL of the weather API.
    - querystring (dict): Query parameters for the API request.
    - headers (dict): Headers for the API request.
    """

    def __init__(self, url, querystring, headers):
        """
        Initializes the WeatherDataFetcher class.

        Parameters:
        - url (str): The URL of the weather API.
        - querystring (dict): Query parameters for the API request.
        - headers (dict): Headers for the API request.
        """
        self.url = url
        self.querystring = querystring
        self.headers = headers

    def fetch_weather_data(self):
        """
        Fetches weather data from the API.

        Returns:
        - list: List of JSON objects containing weather data.
        """
        response = requests.get(self.url, headers=self.headers, params=self.querystring)
        data_json = response.json()['data']
        return data_json

    def store_weather_data_in_redis(self, data_json):
        """
        Stores weather data in Redis.

        Parameters:
        - data_json (list): List of JSON objects containing weather data.
        """
        r = get_redis_connection()
        for i, json_obj in enumerate(data_json):
            redis_key = f"data[{i}]"
            r.execute_command('JSON.SET', redis_key, '.', json.dumps(json_obj))
            print("Stored JSON object at key:", redis_key)
        print("All JSON objects stored in Redis with keys data[0], data[1], ...")

if __name__ == "__main__":
    # Example usage
    url = "https://weatherbit-v1-mashape.p.rapidapi.com/forecast/3hourly"
    querystring = {"lat": "35.5", "lon": "-78.5"}
    headers = {
        "X-RapidAPI-Key": "d9a8ec467dmsh4df4694165689e0p123e67jsn029dad48869c",
        "X-RapidAPI-Host": "weatherbit-v1-mashape.p.rapidapi.com"
    }

    # Create an instance of WeatherDataFetcher
    weather_fetcher = WeatherDataFetcher(url, querystring, headers)
    
    # Fetch weather data from the API
    data_json = weather_fetcher.fetch_weather_data()
    
    # Store weather data in Redis
    weather_fetcher.store_weather_data_in_redis(data_json)
