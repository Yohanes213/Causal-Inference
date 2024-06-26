import unittest
import os
import sys
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join("../Causal-Inference/scripts/")))
from data_pipeline import DataPipeline

pipeline = DataPipeline('Nigeria')

class TestDataPipeline(unittest.TestCase):
    

    def test_read_file(self):
        path = 'tests/sampled_df.csv'
        df = pipeline.read_data(path)
        self.assertEqual(len(df),7)

    def test_save_data(self):
        path = 'tests/sampled_df.csv'
        df = pipeline.read_data(path)
        success = pipeline.save_data(df, path)
        self.assertTrue(success)

    def test_is_weekend(self):
        date = datetime(2023, 1, 1)  # Sunday
        self.assertTrue(pipeline.is_weekend(date))

        date = datetime(2023, 1, 2)  # Monday
        self.assertFalse(pipeline.is_weekend(date))

    def test_is_holiday(self):
        date = datetime(2023, 1, 1) # New Year
        self.assertTrue(pipeline.is_holiday(date))

        date = datetime(2023, 1, 3)
        self.assertFalse(pipeline.is_holiday(date))

    def test_calculate_distance(self):
        origin_str = "6.4316714,3.4555375"
        dest_str = "6.4316714,3.4555375"
        distance = pipeline.calculate_distance(origin_str, dest_str)
        self.assertEqual(distance, 0)


if __name__ == '__main__':
   unittest.main()
