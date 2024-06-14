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

    
    def remove_columns(df, columns):
        try:
            df.drop(columns=columns, inplace=True)

            logger.info(f'Successfully drop {columns}')
            return df
        except Exception as e:
            logger.error(f"Error dropping {columns} columns")

            
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

    def calculate_driver_to_Trip_origin(df_driver, df_completed):
        try:
            for index, row in df_driver.iterows():
                order_id = row['order_id']
                if order_id in df_completed['Trip ID'].unique():
                    origin_lat = df_completed[df_completed['Trip ID'] == order_id]['Origin Lat'].iloc[0]
                    origin_lon = df_completed[df_completed['Trip ID'] == order_id]['Origin Lon'].iloc[0]

                    origin = (origin_lat, origin_lon)

                    driver_loc = (row['lat'], row['lng'])

                    df_driver.at[index, 'Distance From Trip Origin'] = geodesic(origin, driver_loc).km

            logger.info('Sucessfuly Complted calculating the distance from the driver location to trip origin')
            return df_driver

        except Exception as e:
            logger.error("Error Calculating the distance from the driver location to trip origin")



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

