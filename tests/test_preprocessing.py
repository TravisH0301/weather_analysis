import sys
import os
import unittest
from datetime import datetime
import pytz

# Add Python script to the path
script_directory = os.path.abspath("./airflow/dags/scripts")
sys.path.append(script_directory)

from stage_data import pre_process_csv, pre_process_fwf


class TestPreprocessing(unittest.TestCase):
    def test_pre_process_csv(self):
        # Define date variable
        date_today = datetime.now(pytz.timezone("Australia/Melbourne")).date()
        
        # Preprocess test weather dataset
        with open("./tests/test_datasets/melbourne_airport-202310.csv", "rb") as f:
            test_df = pre_process_csv(f, "VIC", date_today)

        # Check if dataframe is not empty
        self.assertFalse(test_df.empty, "The DataFrame should not be empty.")
        # Check if columns are created
        columns = [
            "STATION_NAME",
            "DATE",
            "EVAPO_TRANSPIRATION",
            "RAIN",
            "PAN_EVAPORATION",
            "MAXIMUM_TEMPERATURE",
            "MINIMUM_TEMPERATURE",
            "MAXIMUM_RELATIVE_HUMIDITY",
            "MINIMUM_RELATIVE_HUMIDITY",
            "AVERAGE_10M_WIND_SPEED",
            "SOLAR_RADIATION",
            "STATE",
            "LOAD_DATE"
        ]
        for col in columns:
            self.assertIn(col, test_df.columns, f"{col} column is missing.")


    def test_pre_process_fwf(self):
        # Define date variable
        date_today = datetime.now(pytz.timezone("Australia/Melbourne")).date()

        # Preprocess test station dataset
        with open("./tests/test_datasets/stations_db.txt", "rb") as f:
            test_df = pre_process_fwf(f, date_today)

        # Check if dataframe is not empty
        self.assertFalse(test_df.empty, "The DataFrame should not be empty.")
        # Check if columns are created
        columns = [
            "STATION_ID",
            "STATE",
            "DISTRICT_CODE",
            "STATION_NAME",
            "STATION_SINCE",
            "LATITUDE",
            "LONGITUDE",
            "LOAD_DATE"
        ]
        for col in columns:
            self.assertIn(col, test_df.columns, f"{col} column is missing.")


if __name__ == '__main__':
    unittest.main()
