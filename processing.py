import pandas as pd
from db_config import get_redis_connection
import json
import matplotlib.pyplot as plt

class WeatherAnalyzer:
    """
    A class to analyze weather data retrieved from an API.

    Attributes:
    - r: Redis connection object.
    - json_data_list: List to store retrieved JSON data.
    - df: Pandas DataFrame to store the weather data.
    """

    def __init__(self):
        """
        Initializes the WeatherAnalyzer class.
        """
        self.r = get_redis_connection()
        self.json_data_list = []
        self.df = None

    def fetch_weather_data(self, num_objects=40):
        """
        Fetches weather data from Redis and stores it in json_data_list.

        Parameters:
        - num_objects (int): Number of JSON objects to retrieve (default is 40).
        """
        for i in range(num_objects):
            redis_key = f"data[{i}]"
            json_data = self.r.execute_command('JSON.GET', redis_key)
            if json_data:
                data_obj = json.loads(json_data)
                self.json_data_list.append(data_obj)

    def create_dataframe(self):
        """
        Creates a Pandas DataFrame from the retrieved weather data.
        """
        self.df = pd.DataFrame(self.json_data_list)
        self.df['datetime'] = pd.to_datetime(self.df['datetime'], format='%Y-%m-%d:%H')
        self.df.set_index('datetime', inplace=True)
        self.df['day'] = self.df.index.date

    def plot_daily_temperature(self):
        """
        Plots the average daily temperature.
        """
        daily_temp = self.df.groupby('day')['temp'].mean()
        plt.figure(figsize=(12, 6))
        daily_temp.plot(marker='o', color='blue', linestyle='-')
        plt.title('Average Temperature for Each Day')
        plt.xlabel('Date')
        plt.ylabel('Temperature (°C)')
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    def plot_wind_speed_variation(self):
        """
        Plots the wind speed variation for each day.
        """
        self.df['date'] = self.df.index.date
        unique_dates = self.df['date'].unique()
        num_rows = (len(unique_dates) + 1) // 2
        num_cols = 2
        fig, axs = plt.subplots(num_rows, num_cols, figsize=(15, 10))
        axs = axs.flatten()
        for i, date in enumerate(unique_dates):
            date_filtered_df = self.df[self.df['date'] == date]
            ax = axs[i]
            ax.plot(date_filtered_df.index, date_filtered_df['wind_spd'], marker='o', linestyle='-',
                    label='Wind Speed')
            ax.set_title(f'Wind Speed Variation on {date}')
            ax.set_xlabel('Time')
            ax.set_ylabel('Wind Speed (m/s)')
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True)
            ax.legend()
        plt.tight_layout()
        plt.show()

    def classify_weather(self, clouds):
        """
        Classifies weather conditions based on the cloud cover.

        Parameters:
        - clouds (float): Cloud cover percentage.

        Returns:
        - str: Weather condition category ('Cloudy', 'Moderate', or 'Sunny').
        """
        if clouds > 80:
            return 'Cloudy'
        elif 60 <= clouds <= 80:
            return 'Moderate'
        else:
            return 'Sunny'

    def print_weather_conditions(self):
        """
        Prints the most frequent weather condition for each date.
        """
        self.df['weather_condition'] = self.df['clouds'].apply(self.classify_weather)
        weather_by_date = self.df.groupby(self.df.index.date)['weather_condition'].agg(lambda x: x.value_counts().index[0])
        for date, weather in weather_by_date.items():
            print(f"{date} - {weather}")


if __name__ == "__main__":
    analyzer = WeatherAnalyzer()
    analyzer.fetch_weather_data()
    analyzer.create_dataframe()
    analyzer.plot_daily_temperature()
    analyzer.plot_wind_speed_variation()
    analyzer.print_weather_conditions()
