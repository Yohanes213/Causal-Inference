import pandas as pd
from datetime import datetime
import holidays
from geopy.distance import geodesic
import logging
import numpy as np

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('log.log')
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class DataPipeline:
    def __init__(self, country='Nigeria'):
        """
        Initializes the DataPipeline class.

        Parameters:
            country (str): The country for holiday calculations. Default is 'Nigeria'.
        """
        self.country = country
        self.holidays = holidays.CountryHoliday(self.country)
        logger.info(f"DataPipeline instance created for country: {self.country}")

    def read_data(self, path):
        """
        Reads a CSV file and returns a DataFrame.

        Parameters:
            path (str): The path to the file.

        Returns:
            pd.DataFrame: The pandas DataFrame containing the data from the file.
        """
        try:
            df = pd.read_csv(path)
            logger.info(f"File has been successfully read from {path}")
            return df
        except Exception as e:
            logger.error(f"Error reading the file: {e}")
            return None
        
    def save_data(self, df, path):
        """
        Saves a DataFrame to a file.

        Parameters:
            df (pd.DataFrame): The DataFrame to be saved.
            path (str): The path where the DataFrame should be saved.

        Returns:
            bool: True if the DataFrame is saved successfully, False otherwise.
        """
        try:
            df.to_csv(path, index=False)
            logger.info(f"DataFrame has been successfully saved to {path}")
            return True
        except Exception as e:
            logger.error(f"Error saving the DataFrame to {path}: {e}")
            return False

    def is_weekend(self, date):
        """
        Checks if a given date is a weekend.

        Parameters:
            date (datetime): The date to check.

        Returns:
            bool: True if the date is a weekend (Saturday or Sunday), False otherwise.
        """
        try:
            return date.weekday() >= 5
        except Exception as e:
            logger.error(f"Error checking if date is weekend: {e}")
            return False

    def is_holiday(self, date):
        """
        Checks if a given date is a holiday.

        Parameters:
            date (datetime): The date to check.

        Returns:
            bool: True if the date is a holiday, False otherwise.
        """
        try:
            return date in self.holidays
        except Exception as e:
            logger.error(f"Error checking if {date} column is holiday: {e}")
            return False

    def change_datetime(self, df, date_columns):
        """
        Changes specified columns to datetime format in the DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame containing the data.
            date_columns (list): A list of column names to be converted to datetime.

        Returns:
            pd.DataFrame: The DataFrame with specified columns converted to datetime format.
        """
        try:
            for column in date_columns:
                df[column] = pd.to_datetime(df[column])
            logger.info(f"Converted columns {date_columns} to datetime format")
            return df
        except Exception as e:
            logger.error(f"Error converting columns to datetime: {e}")
            return df
        
    def calculate_distance(self, origin_str, destination_str):
        """
        Calculates the distance between two geographic coordinates.

        Parameters:
            origin_str (str): The string containing the origin latitude and longitude in "lat,lon" format.
            destination_str (str): The string containing the destination latitude and longitude in "lat,lon" format.

        Returns:
            float: The calculated distance in kilometers.
        """
        try: 
            origin_lat, origin_lon = map(float, origin_str.split(","))
            dest_lat, dest_lon = map(float, destination_str.split(","))

            origin = (origin_lat, origin_lon)
            destination = (dest_lat, dest_lon)

            distance = geodesic(origin, destination).km

            return distance
        except Exception as e:
            logger.error(f"Error converting {origin_str, destination_str}: {e}")

    def calculate_duration(self, df, start_time, end_time):
        """
        Calculates the duration in hours between two datetime columns in the DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame containing the data.
            start_time (str): The column name containing the start time.
            end_time (str): The column name containing the end time.

        Returns:
            pd.Series: The duration in hours for each row.
        """
        try:
            return (df[end_time] - df[start_time]) / pd.Timedelta(hours=1)
        except Exception as e:
            logger.error(f"Error calculating the duration for {start_time} {end_time}: {e}")

    def remove_columns(self, df, columns):
        """
        Removes specified columns from the DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame from which columns are to be removed.
            columns (list): The list of column names to be removed.

        Returns:
            pd.DataFrame: The DataFrame with specified columns removed.
        """
        try:
            df.drop(columns=columns, inplace=True)
            logger.info(f'Successfully dropped columns: {columns}')
            return df
        except Exception as e:
            logger.error(f"Error dropping columns {columns}: {e}")

    def impute_elements(self, df, distance, column):
        """
        Imputes missing elements in a specified column based on the nearest neighbor approach.

        Parameters:
            df (pd.DataFrame): The DataFrame containing the data.
            distance (np.ndarray): The distance matrix to find the nearest neighbor.
            column (str): The column name where missing values need to be imputed.

        Returns:
            pd.DataFrame: The DataFrame with imputed values in the specified column.
        """
        try:
            for i, row in df.iterrows():
                if pd.isnull(row[column]):
                    closest_index = np.argmin(distance[i])
                    df.at[i, column] = df.at[closest_index, column]
            logger.info(f'Successfully imputed column: {column}')
            return df
        except Exception as e:
            logger.error(f'Error imputing column {column}: {e}')

    def remove_outliers(self, df, column, lower_quantile=0.25, upper_quantile=0.75):
        """
        Removes outliers from a specified column in the DataFrame using the IQR method.

        Parameters:
            df (pd.DataFrame): The DataFrame containing the data.
            column (str): The column name from which to remove outliers.
            lower_quantile (float): The lower quantile to calculate IQR. Default is 0.25.
            upper_quantile (float): The upper quantile to calculate IQR. Default is 0.75.

        Returns:
            pd.DataFrame: The DataFrame with outliers removed based on the specified column.
        """
        try:
            Q1 = df[column].quantile(lower_quantile)
            Q3 = df[column].quantile(upper_quantile)

            IQR = Q3 - Q1

            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            df_no_outliers = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
            logger.info(f'Successfully removed outliers using column {column}')
            return df_no_outliers
        except Exception as e:
            logger.error(f"Error removing outliers using column {column}: {e}")

    def calculate_driver_to_Trip_origin(self, df_driver, df_completed):
        """
        Calculates the distance from the driver's location to the trip origin for each driver.

        Parameters:
            df_driver (pd.DataFrame): The DataFrame containing driver information.
            df_completed (pd.DataFrame): The DataFrame containing completed trip information.

        Returns:
            pd.DataFrame: The DataFrame with the distance from the driver's location to the trip origin added.
        """
        try:
            for index, row in df_driver.iterrows():
                order_id = row['order_id']
                if order_id in df_completed['Trip ID'].unique():
                    origin_lat = df_completed[df_completed['Trip ID'] == order_id]['Origin Lat'].iloc[0]
                    origin_lon = df_completed[df_completed['Trip ID'] == order_id]['Origin Lon'].iloc[0]

                    origin = (origin_lat, origin_lon)
                    driver_loc = (row['lat'], row['lng'])

                    df_driver.at[index, 'Distance From Trip Origin'] = geodesic(origin, driver_loc).km

            logger.info('Successfully completed calculating the distance from the driver location to trip origin')
            return df_driver
        except Exception as e:
            logger.error("Error calculating the distance from the driver location to trip origin: {e}")
