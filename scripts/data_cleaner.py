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

        Params:
            country (str): The country for holiday calculations. Default is 'Nigeria'.
        """
        self.country = country
        self.holidays = holidays.CountryHoliday(self.country)
        logger.info(f"DataPipeline instance created for country: {self.country}")

    def read_data(self, path):
        """
        Reads a CSV file and returns a DataFrame.

        Params:
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
        Save a DataFrame to a file.

        Params:
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

        Params:
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

        Params:
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

        Params:
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
        
    def apply_weekend_holiday(self, df, date_columns):
        """
        Applies weekend and holiday checks to specified date columns in a DataFrame 
        and adds new columns indicating the results.

        Params:
            df (pd.DataFrame): The DataFrame containing the data.
            date_columns (list): A list of column names containing date values.

        Returns:
            pd.DataFrame: The original DataFrame with additional columns for weekend and holiday indicators.
        """
        try:
            df = self.change_datetime(df, date_columns)
            
            column = date_columns[1]
            
            df[f'is_weekend'] = df[column].apply(self.is_weekend)
            logger.info(f"Applied weekend check to DataFrame on column {column}")
            
            df[f'is_holiday'] = df[column].apply(self.is_holiday)
            logger.info(f"Applied holiday check to DataFrame on column {column}")
            
            logger.info(f"Weekend and holiday checks applied to {len(df)} rows.")
            return df
        except Exception as e:
            logger.error(f"Error applying weekend and holiday checks: {e}")
            return df
        
    def calculate_distance(self, origin_str, destination_str):
        """
        Calculate the distance from the lat and lon.

        params:
            df (pd.DataFrmae): The dataframe containing the data.
            origin_str (str): The data containing the origin lat and lon.
            destination_str: The data containing the destination lat and lon.
        
        Returns:
            distance (float): The calculated distance in km. 
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

    def calculate_duration(self, df,start_time, end_time):
        """
        Calculate the duration in hours.

        params:
            df (pd.DataFrmae): The dataframe containing the data.
            start_time (pd.DataFrame): The data containing the start time.
            end_time (pd.DataFrame): The data containing the end time.
        
        Returns:
            duration (float): The duration time 
        """

        try:
            return (df[end_time] - df[start_time])/ pd.Timedelta(hours=1)
        except Exception as e:
            logger.error(f"Error Calucation the duration for {start_time} {end_time}: {e}")

    def apply_distance_duration(self, df, origin_cols, destination_cols, start_time_col, end_time_col):
        """
        Applies distance and duration calculations to the DataFrame and adds new columns for distance, duration, and speed.

        Params:
            df (pd.DataFrame): The DataFrame containing the data.
            origin_cols (list): A list of column names containing origin latitudes and longitudes.
            destination_cols (list): A list of column names containing destination latitudes and longitudes.
            start_time_col (str): The column name containing the start time.
            end_time_col (str): The column name containing the end time.

        Returns:
            pd.DataFrame: The original DataFrame with additional columns for distance, duration, and speed.
        """
        try:
            for origin_col, destination_col in zip(origin_cols, destination_cols):
                df[f'Distance (km)'] = df.apply(
                    lambda row: self.calculate_distance(row[origin_col], row[destination_col]), axis=1)
                logger.info(f"Applied distance calculation to DataFrame for {origin_col} and {destination_col}")

            df['Duration (hr)'] = self.calculate_duration(df, start_time_col, end_time_col)
            logger.info(f"Applied duration calculation to DataFrame using {start_time_col} and {end_time_col}")

            df['Speed (km/hr)'] = df['Distance (km)'] / df['Duration (hr)']
            logger.info("Calculated speed")

            logger.info(f"Distance, duration, and speed calculations applied to {len(df)} rows.")
            return df
        except Exception as e:
            logger.error(f"Error applying distance, duration, and speed calculations: {e}")
            return df
    
    def impute_elements(df, distance, column):
        try:
            for i, row in df.iterrows():
                if pd.null(row[column]):
                    closet_index = np.argmin(distance[i])
                    df.at[i, column] = df.at[closet_index, column]
            logger.info(f'Succesfully imputed {column}')
            return df
        
        except Exception as e:
            logger.error(f'Error imputing {column}')

    
    def remove_outliers(df, column, lower_quantile = 0.25, upper_quantile = 0.75):
        try:
            Q1 = df[column].quantile(lower_quantile)
            Q3 = df[column].quantile(upper_quantile)

            IQR = Q3 - Q1

            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            df_no_outliers = df[df[column] >= lower_bound & df[column] <= upper_bound]

            logger.info(f'Succesfully removed outliers using column {column}')
            return df_no_outliers
        
        except Exception as e:
            logger.error('Error Removing outliers')

    
# Usage example
# if __name__ == "__main__":
#     pipeline = DataPipeline(country='Nigeria')
#     df = pipeline.read_data('data/nb.csv')

#     if df is not None:
#         df = pipeline.apply_weekend_holiday(df, ['Trip Start Time', 'Trip End Time'])
#         logger.info("Weekend and holiday checks have been successfully applied to the DataFrame")
#         print(df.head())
#         print(df['is_holiday'].value_counts()) 
#         print(df['is_weekend'].value_counts())  

if __name__ == "__main__":
    pipeline = DataPipeline('Nigeria')
    df = pipeline.read_data('data/nb.csv')

    if df is not None:
        # Define the columns for datetime conversion and weekend/holiday checks
        date_columns = ['Trip Start Time', 'Trip End Time']
        df = pipeline.apply_weekend_holiday(df, date_columns)
        
        # Define columns for distance and duration calculation
        origin_cols = ['Trip Origin']
        destination_cols = ['Trip Destination']
        # start_time_col = 'start_time'
        # end_time_col = 'end_time'

        df = pipeline.apply_distance_duration(df, origin_cols, destination_cols, date_columns[0], date_columns[1])

        pipeline.save_data(df, 'data/cleaned_nb.csv')
        #logger.info("Distance, duration, and speed calculations have been applied to the DataFrame")
        print(df.head())  # Display the first few rows of the DataFrame to verify changes
        print(df.info())