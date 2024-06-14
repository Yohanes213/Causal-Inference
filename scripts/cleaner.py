from data_cleaner import DataPipeline
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('log.log')
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def apply_distance_duration(pipeline, df, origin_cols, destination_cols, start_time_col, end_time_col):
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
                lambda row: pipeline.calculate_distance(row[origin_col], row[destination_col]), axis=1)
            logger.info(f"Applied distance calculation to DataFrame for {origin_col} and {destination_col}")

        df = pipeline.impute_elements(df, df['Distance (km)'], start_time_col)
        df = pipeline.impute_elements(df, df['Distance (km)'], end_time_col)

        df['Duration (hr)'] = pipeline.calculate_duration(df, start_time_col, end_time_col)
        logger.info(f"Applied duration calculation to DataFrame using {start_time_col} and {end_time_col}")

        df['Speed (km/hr)'] = df['Distance (km)'] / df['Duration (hr)']
        logger.info("Calculated speed")

        logger.info(f"Distance, duration, and speed calculations applied to {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Error applying distance, duration, and speed calculations: {e}")
        return df
    
def apply_weekend_holiday(pipeline, df, date_columns):
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
        df = pipeline.change_datetime(df, date_columns)
        
        column = date_columns[1]
        
        df[f'is_weekend'] = df[column].apply(pipeline.is_weekend)
        logger.info(f"Applied weekend check to DataFrame on column {column}")
        
        df[f'is_holiday'] = df[column].apply(pipeline.is_holiday)
        logger.info(f"Applied holiday check to DataFrame on column {column}")
        
        logger.info(f"Weekend and holiday checks applied to {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Error applying weekend and holiday checks: {e}")
        return df
    

if __name__ == "__main__":
    pipeline = DataPipeline('Nigeria')
    df_completed = pipeline.read_data('data/nb.csv')
    df_driver = pipeline.read_data('data/driver_locations_during_request.csv')
    print(df_completed.head())
    print(df_driver.head())

    if df_completed is not None:
        # Define the columns for datetime conversion and weekend/holiday checks
        df_driver = pipeline.remove_columns(df_driver, columns = ['created_at','updated_at'])
        print(df_driver.head())
        date_columns = ['Trip Start Time', 'Trip End Time']
        df_completed = apply_weekend_holiday(pipeline, df_completed, date_columns)
        
        print(df_completed.head())
        # Define columns for distance and duration calculation
        origin_cols = ['Trip Origin']
        destination_cols = ['Trip Destination']
        # start_time_col = 'start_time'
        # end_time_col = 'end_time'

        df_completed = apply_distance_duration(pipeline, df_completed, origin_cols, destination_cols, date_columns[0], date_columns[1])

        print(df_completed)
        df_no_outliers = pipeline.remove_outliers(df_completed, 'Speed (km/hr)')

        print(df_no_outliers)
        df = pipeline.calculate_driver_to_Trip_origin(df_driver, df_no_outliers)

        df_completed_lookup = df_completed.set_index('Trip ID')[['Distance (km)', 'is_weekend', 'is_holiday']]

        df = df.merge(df_completed_lookup, how='left', left_on='order_id', right_index=True)

        df.rename(columns={'Distance (km)': 'Distance Trip to Destination'}, inplace=True)

        pipeline.save_data(df, 'data/cleaned_df.csv')
 