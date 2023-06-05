"""
test_main.py
------------
This code piece is to test the functions and methods created for the pipeline
Tests have been written with Python unitest case methods. Using setUpClass and tearDown
"""

import os
import sys

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(ROOT_DIR)

import unittest
from utilities import helper
from pyspark.sql.types import *
from pyspark.sql import SparkSession


class SparkETLTestCase(unittest.TestCase):
    """Test suite for extraction method"""

    @classmethod
    def setUpClass(self):

        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        self.spark = SparkSession.builder \
            .appName("UnitTest") \
            .master("local[*]") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

    @classmethod
    def tearDown(self):
        """Stop Spark"""
        self.spark.stop()

    # This function is used to test the deduplication function in utilities.
    def test_dedup(self):

        #dummy dataset
        data = [
            (3794,"United States", 57, 'Other'),
            (3844,"France", 62, 'Female'),
            (3794,"United States", 57, 'Other'),
            (3844,"France", 62, 'Female')
        ]

        schema = StructType([
            StructField('customer_id', IntegerType(), True),
            StructField('country', StringType(), True),
            StructField('customer_age', IntegerType(), True),
            StructField('customer_gender', StringType(), True)
        ])

        input_df = self.spark.createDataFrame(data = data, schema = schema)

        # Define partition and order by columns
        partition_cols = ["customer_id"]
        order_by_cols = ["customer_id"]

        # Execute the dedup function
        result_df = helper.dedup(input_df, partition_cols, order_by_cols)

        # Define expected output DataFrame
        expected_data = [
            (3794,"United States", 57, 'Other'),
            (3844,"France", 62, 'Female')
        ]
        expected_df = self.spark.createDataFrame(data = expected_data, schema = schema)

        # Perform assertions on the result DataFrame
        self.assertEqual(sorted(expected_df.collect()), sorted(result_df.collect()))

    # This test is to check the transformation of currency conversion.
    def test_currency_conversion(self):
        # Create dummy input DataFrame
        data = [
            (1238, "GBP", "USD", '2020-02-16', 1.305168),
            (6392, "GBP", "JPY", '2020-02-16', 143.300879),
            (1239, "GBP", "GBP", '2020-02-16', 1.0),
            (1237, "GBP", "EUR", '2020-02-16', 1.203874)
        ]

        schema = StructType([
            StructField('exchange_rate_id', IntegerType(), True),
            StructField('from_currency', StringType(), True),
            StructField('to_currency', StringType(), True),
            StructField('effective_date', StringType(), True),
            StructField('rate', DoubleType(), True)
        ])

        input_df = self.spark.createDataFrame(data = data, schema = schema)

        # Define the currency to convert to
        to_currency = "USD"

        # Execute the currency_conversion function
        result_df = helper.currency_conversion(input_df, to_currency)
        result_df.show()

        # Define expected output DataFrame
        expected_data = [
            ("2020-02-16", "USD", "USD", 1.0),
            ("2020-02-16", "JPY", "USD", 0.009),
            ("2020-02-16", "GBP", "USD", 1.305),
            ("2020-02-16", "EUR", "USD", 1.084)
        ]

        expected_df = self.spark.createDataFrame(expected_data,
                                                 ["effective_date", "from_currency", "to_currency", "rate"])

        # Perform assertions on the result DataFrame
        self.assertEqual(sorted(expected_df.collect()), sorted(result_df.collect()))


if __name__ == '__main__':
    unittest.main()