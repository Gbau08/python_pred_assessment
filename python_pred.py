import requests
import pandas as pd
from io import BytesIO
import zipfile
import pyodbc
import time
import logging
import os


# Credentials are stored in Azure, so it is not visible on git
DATABASE_CREDENTIALS = {
    'server': os.environ.get('DATABASE_HOST', 'default_host_if_not_set'),
    'database': 'SQL_DB_01_DEV',
    'username': os.environ.get('DATABASE_USER', 'default_user_if_not_set'),
    'password': os.environ.get('DATABASE_PASSWORD', 'default_password_if_not_set'),
    'driver': '{ODBC Driver 17 for SQL Server}'
}

DATA_URL = "https://drive.google.com/uc?export=download&id=1bH6BM7hrVI9ufuJ5GVGE7QPEwIJAM1xX"


class DataFetcher:

    @staticmethod
    def download_data(url, retries=5, delay=60):
        """
        Download the data from the provided URL.
        Retry mechanism in place that waits for the specified delay before the next attempt
        """
        for _ in range(retries):
            try:
                logging.info(f"Trying to download from {url}")
                response = requests.get(url, allow_redirects=True)
                # Ensure the response status is successful (e.g., 200 OK)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logging.error(f"Attempt failed. Error: {e}")
                # If this wasn't the last retry attempt, wait for the specified delay before the next attempt
                if _ < retries - 1:
                    logging.info(f"Waiting for {delay} seconds before retrying...")
                    time.sleep(delay)
                else:
                    # If all retries have been exhausted, log an error and return None
                    logging.error("Max retries reached. Exiting.")
                    return None


    @staticmethod
    def extract_data_from_zip(response):
        """
        Extract the data from the zip file and create Panda dataframes out of those files.
        """

        try:
            # Open the provided zip file using a BytesIO buffer
            with zipfile.ZipFile(BytesIO(response.content)) as z:
                # Get a list of all file names within the zip archive
                files_in_zip = z.namelist()

                # Identify and load .ndjson files from the zip archive
                json_files = [file_name for file_name in files_in_zip if file_name.endswith('.ndjson')]
                jsonDfs = []
                for file_name in json_files:
                    # Read each .ndjson file into a pandas DataFrame
                    df = pd.read_json(z.open(file_name), lines=True)
                    jsonDfs.append(df)
                    logging.info(f"Loaded {len(df)} rows from {file_name}")

                # Validate that all identified .ndjson files were successfully loaded
                if len(jsonDfs) != len(json_files):
                    raise ValueError(f"Expected to load {len(json_files)} .ndjson files but loaded {len(jsonDfs)}")

                # Combine all the .ndjson DataFrames into a single DataFrame
                combined_df = pd.concat(jsonDfs, ignore_index=True)
                logging.info(f"Combined .ndjson files into a DataFrame with {len(combined_df)} rows")

                # Load the countries.csv file from the zip archive, if it exists
                country_files = [file_name for file_name in files_in_zip if file_name.endswith('countries.csv')]
                if country_files:
                    countries_df = pd.read_csv(z.open(country_files[0]))
                else:
                    raise ValueError("No countries.csv file found in the zip archive.")

                # Return the combined .ndjson DataFrame and countries DataFrame
                return combined_df, countries_df
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return None, None


class DataProcessor:

    @staticmethod
    def filter_countries(combined_df, countries_df):
        """
        Filter the combined dataset using the list of countries from the countries_df.
        Use join to speed up processing
        """
        try:
            combined_df['country'] = combined_df['country'].str.upper().str.strip()
            countries_df['country_code'] = countries_df['country_code'].str.upper().str.strip()
            filtered_countries_df = pd.merge(combined_df, countries_df, left_on='country', right_on='country_code', how='inner')
            logging.info(f"Filtered DataFrame by countries now {len(filtered_countries_df)} rows")
            return filtered_countries_df
        except Exception as e:
            logging.error(f"Error during Data filtering & processing: {e}")
            return None
    
    @staticmethod
    def filter_parameters_hourly_obs(df):
        """
        Filter the dataset to keep only specific parameters and observations made on an hourly basis.
        Assumption: to compute the 24h rollling average, we only take records where the observations are made very hour (remove the other unit such as 8 or 24h average)
        """
        try:
            filtered_df = df.copy()
            filtered_df['parameter'] = filtered_df['parameter'].str.strip().str.upper() # Make sure to trim & upper case for filter below
            filtered_df = filtered_df[filtered_df['parameter'].isin(['PM2.5', 'PM10', 'O3', 'NO2', 'CO'])] # As per requirement
            filtered_df['date'] = filtered_df['date'].apply(lambda x: x['utc'])
            filtered_df['date'] = pd.to_datetime(filtered_df['date'])
            filtered_df['averagingPeriod_value'] = filtered_df['averagingPeriod'].apply(lambda x: x.get('value', None))
            filtered_df['averagingPeriod_unit'] = filtered_df['averagingPeriod'].apply(lambda x: x.get('unit', None))
            filtered_df = filtered_df[(filtered_df['averagingPeriod_value'] == 1) & (filtered_df['averagingPeriod_unit'] == 'hours')]
            logging.info(f"Filtered DataFrame by paramter and hourly unit only, now {len(filtered_df)} rows")
            return filtered_df
        except Exception as e:
            logging.error(f"Error filter_parameters_hourly_obs: {e}")
            return None
    
    @staticmethod
    def compute_rolling_average(df):
        """
        Compute the 24h rolling average for the filtered data.
        Assumptions: multiple observations per city at the same time, we will take the mean of those obversations to have an hourly average per city/date/time
        then the 24h rolling average is computed. If there are less then 24 observations, we  still compute the rolling average of those observations
        """
        try:
            filtered_df = DataProcessor.filter_parameters_hourly_obs(df)
            city_hourly_avg_df = filtered_df.groupby(['city', 'date', 'parameter']).agg({'value': 'mean'}).reset_index()
            city_hourly_avg_df.sort_values(by=['city','parameter','date'], inplace=True)
            rolling_avg_df = (city_hourly_avg_df
                              .groupby(['city', 'parameter'])
                              .apply(lambda group: group.set_index('date').resample('H')['value'].mean().rolling(window=24, min_periods=1).mean())
                              .reset_index()
                              .rename(columns={'value': '24hr_avg'}))
            logging.info(f"Computed Rolling Avg DataFrame, now {len(rolling_avg_df)} rows")
            return rolling_avg_df
        except Exception as e:
            logging.error(f"Error compute_rolling_average: {e}")
            return None
    
    @staticmethod
    def calculate_aqi_formula(concentration, breakpoints):
        """
        Compute the Air Quality Index (AQI) values based on the 24h rolling averages.
        The AQI data is stored in the aqi_df instance attribute.
        """
        try:
            for category, values in breakpoints.items():
                BreakpointLow, BreakpointHigh = values['Concentration']
                AQILow, AQIHigh = values['AQI']
                if BreakpointLow <= concentration <= BreakpointHigh:
                    AQI = ((AQIHigh - AQILow) / (BreakpointHigh - BreakpointLow)) * (concentration - BreakpointLow) + AQILow
                    return AQI, category
            return None, None  # Return None if concentration doesn't fit in any category
        except Exception as e:
            logging.error(f"Error calculate_aqi_formula: {e}")
            return None, None

    @staticmethod
    def compute_aqi(rolling_avg_df):
 
        # Defining AQI breakpoints and categories for PM2.5 and PM10 (could be placed elsewhere in the future)
        pm25_breakpoints = {
            'Good': {'AQI': (0, 50), 'Concentration': (0, 12.0)},
            'Moderate': {'AQI': (51, 100), 'Concentration': (12.1, 35.4)},
            'Unhealthy for Sensitive Groups': {'AQI': (101, 150), 'Concentration': (35.5, 55.4)},
            'Unhealthy': {'AQI': (151, 200), 'Concentration': (55.5, 150.4)},
            'Very Unhealthy': {'AQI': (201, 300), 'Concentration': (150.5, 250.4)},
            'Hazardous': {'AQI': (301, 500), 'Concentration': (250.5, 500.4)},
            'Beyond AQI': {'AQI': (501, 999), 'Concentration': (500.5, 99999.9)}
        }
        pm10_breakpoints = {
            'Good': {'AQI': (0, 50), 'Concentration': (0, 54.0)},
            'Moderate': {'AQI': (51, 100), 'Concentration': (55.0, 154.0)},
            'Unhealthy for Sensitive Groups': {'AQI': (101, 150), 'Concentration': (155.0, 254.0)},
            'Unhealthy': {'AQI': (151, 200), 'Concentration': (255.0, 354.0)},
            'Very Unhealthy': {'AQI': (201, 300), 'Concentration': (355.0, 424.0)},
            'Hazardous': {'AQI': (301, 500), 'Concentration': (425.0, 504.0)},
            'Beyond AQI': {'AQI': (501, 999), 'Concentration': (505.0, 99999.9)}
        }

        try:
            aqi_df = rolling_avg_df.copy()
            aqi_df = aqi_df[aqi_df['parameter'].isin(['PM25', 'PM10'])]
            aqi_df[['AQI', 'AQI_Category']] = aqi_df.apply(lambda row: DataProcessor.calculate_aqi_formula(row['24hr_avg'], pm25_breakpoints) if row['parameter'] == 'PM25' else DataProcessor.calculate_aqi_formula(row['24hr_avg'], pm10_breakpoints), axis=1, result_type='expand')
            return aqi_df
        except Exception as e:
            logging.error(f"Error computing AQI values: {e}")



class DatabaseManager:

    def __init__(self, connection_str):
        self.connection = pyodbc.connect(connection_str)
        self.cursor = self.connection.cursor()

    def create_tables_if_not_exists(self, aqi_table_name="AQI_TABLE", rolling_avg_table_name="ROLLING_AVG_TABLE"):
        logging.info(f"Creating tables if they do not exist: {aqi_table_name}, {rolling_avg_table_name}")
        
        # Queries for AQI_TABLE
        aqi_table_check = f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{aqi_table_name}')"
        aqi_table_creation = f"""
        BEGIN
            CREATE TABLE {aqi_table_name} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                city NVARCHAR(100),
                parameter NVARCHAR(50),
                date DATETIMEOFFSET,
                [24hr_rolling_avg] FLOAT,
                AQI FLOAT,
                AQI_Category NVARCHAR(100)
            )
        END
        """
        
        # Execute
        self.cursor.execute(aqi_table_check + aqi_table_creation)
        logging.info(f"{aqi_table_name} checked/created successfully.")
        
        # Indexes for AQI_TABLE
        self.cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_city') CREATE INDEX idx_city ON {aqi_table_name}(city)")
        self.cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_parameter') CREATE INDEX idx_parameter ON {aqi_table_name}(parameter)")
        self.cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_date') CREATE INDEX idx_date ON {aqi_table_name}(date)")
        logging.info(f"Indexes for {aqi_table_name} checked/created successfully.")
        
        # Queries for ROLLING_AVG_TABLE
        rolling_avg_table_check = f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{rolling_avg_table_name}')"
        rolling_avg_table_creation = f"""
        BEGIN
            CREATE TABLE {rolling_avg_table_name} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                city NVARCHAR(100),
                parameter NVARCHAR(50),
                date DATETIMEOFFSET,
                [24hr_rolling_avg] FLOAT
            )
        END
        """
        
        # Execute
        self.cursor.execute(rolling_avg_table_check + rolling_avg_table_creation)
        logging.info(f"{rolling_avg_table_name} checked/created successfully.")
        
        # Indexes for ROLLING_AVG_TABLE
        self.cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx2_city') CREATE INDEX idx2_city ON {rolling_avg_table_name}(city)")
        self.cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx2_parameter') CREATE INDEX idx2_parameter ON {rolling_avg_table_name}(parameter)")
        logging.info(f"Indexes for {rolling_avg_table_name} checked/created successfully.")
        self.cursor.commit()

    def batch_insert_data(self, aqi_df, rolling_avg_df):
        logging.info("Batch inserting data to AQI_TABLE and ROLLING_AVG_TABLE")
        
        # Insert data into the AQI_TABLE table
        aqi_to_insert = list(aqi_df.itertuples(index=False, name=None))
        self.cursor.executemany("INSERT INTO AQI_TABLE (city, parameter, date, [24hr_rolling_avg], AQI, AQI_Category) VALUES (?, ?, ?, ?, ?, ?)", aqi_to_insert)
        self.cursor.commit()
        logging.info("Data inserted into AQI_TABLE successfully.")
        
        # Insert data into the ROLLING_AVG_TABLE
        rolling_avg_to_insert = list(rolling_avg_df.itertuples(index=False, name=None))
        self.cursor.executemany("INSERT INTO ROLLING_AVG_TABLE (city, parameter, date, [24hr_rolling_avg]) VALUES (?, ?, ?, ?)", rolling_avg_to_insert)
        self.cursor.commit()


    def drop_table_if_exists(self, table_name):
        logging.info(f"Dropping table if exists: {table_name}")
        
        drop_query = f"""
        IF OBJECT_ID('dbo.{table_name}', 'U') IS NOT NULL
            DROP TABLE dbo.{table_name};
        """
        self.cursor.execute(drop_query)
        self.cursor.commit()
        logging.info(f"{table_name} dropped successfully.")

    def close(self):
        if self.connection:
            logging.info(f"Closing the database connection")
            self.connection.close()
            self.connection = None
            self.cursor = None


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Fetch Data
    fetcher = DataFetcher()
    response = fetcher.download_data(DATA_URL)
    if response.status_code==200:
        combined_df, countries_df = fetcher.extract_data_from_zip(response)
        # Process Data
        processor = DataProcessor()
        filtered_countries_df = processor.filter_countries(combined_df, countries_df)
        rolling_avg_df = processor.compute_rolling_average(filtered_countries_df)
        aqi_df = processor.compute_aqi(rolling_avg_df)
        # Database Operations
        connection_str = f"DRIVER={DATABASE_CREDENTIALS['driver']};SERVER={DATABASE_CREDENTIALS['server']};DATABASE={DATABASE_CREDENTIALS['database']};UID={DATABASE_CREDENTIALS['username']};PWD={DATABASE_CREDENTIALS['password']}"
        db_manager = DatabaseManager(connection_str)
        db_manager.drop_table_if_exists('AQI_TABLE')
        db_manager.drop_table_if_exists('ROLLING_AVG_TABLE')
        db_manager.create_tables_if_not_exists("AQI_TABLE", "ROLLING_AVG_TABLE")
        db_manager.batch_insert_data(aqi_df, rolling_avg_df)
        db_manager.close()



if __name__ == "__main__":
    main()

