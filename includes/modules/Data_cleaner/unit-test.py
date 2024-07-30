import os, sys
sys.path.insert(1, os.path.join(os.getcwd()))

import unittest
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, DoubleType

from modules.Data_cleaner.Strategies import *
from modules.Data_cleaner.Interface import *


import warnings
warnings.simplefilter(action='ignore')

# Assuming the strategies and pipeline classes are defined as above

class TestCleaningStrategies(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("DataCleaningTests") \
            .master("local[*]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("OFF")
        

        # Constructing a test DataFrame
        data = [
            (1, "Alice", 30, "2024-07-01", "alice@example.com"),
            (2, "Bob", None, "2024-07/02", "bob@example"),
            (3, "Charlie", 25, "invalid_date", None),
            (4, "David", -5, "2024-07-04", "david@example.com"),
            (5, "Eve", 22, "2024-07-05", "eve@example.com"),
            (5, "Eve", 22, "2024-07-05", "eve@example.com"),  # Duplicate row
        ]
        schema = ["id", "name", "age", "date", "email"]
        cls.df = cls.spark.createDataFrame(data, schema=schema)

    def test_drop_duplicates(self):
        strategy = DropDuplicatesStrategy(columns=self.df.columns)
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        result = cleaned_df.count()
        self.assertEqual(result, 5)  # Should drop the duplicate row

    def test_drop_missing_values(self):
        strategy = DropMissingValuesStrategy(columns=["age"])
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        result = cleaned_df.filter(col("age").isNull()).count()
        self.assertEqual(result, 0)  # Should drop rows with null values in 'age'

    def test_validate_column_types(self):
        strategy = ValidateColumnTypesStrategy(columns=self.df.columns, expected_types={'age': 'IntegerType'})
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        errors = json.loads(strategy.get_report())
        self.assertGreater(len(errors['errors']), 0)  # Should log errors due to invalid type in 'age'

    def test_validate_dates(self):
        strategy = ValidateDatesStrategy(columns=["date"], date_format='yyyy-MM-dd')
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        errors = json.loads(strategy.get_report())
        self.assertGreater(len(errors['errors']), 0)  # Should log errors due to invalid date format

    def test_validate_regex(self):
        strategy = ValidateRegexStrategy(columns=["email"], patterns={'email': '^[\\w.%+-]+@[\\w.-]+\\.[a-zA-Z]{2,}$'})
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        errors = json.loads(strategy.get_report())
        self.assertGreater(len(errors['errors']), 0)  # Should log errors for invalid email formats

    def test_filter_negative_values(self):
        strategy = FilterNegativeValuesStrategy(columns=["age"])
        pipeline = CleaningPipeline()
        pipeline.add_strategy(strategy)
        pipeline.set_dataframe(self.df)

        cleaned_df = pipeline.run()
        result = cleaned_df.filter(col("age") < 0).count()
        self.assertEqual(result, 0)  # Should filter out negative values in 'age'

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
