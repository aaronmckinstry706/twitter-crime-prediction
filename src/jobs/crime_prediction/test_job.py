import unittest

import pyspark
import pyspark.sql as sql

from jobs.crime_prediction import run_job as run_job

class TestJob(unittest.TestCase):
    """This class is for testing any dataframe/rdd transformations which can't be tested without
    a SparkContext/SparkSession."""
    
    @classmethod
    def setUpClass(cls):
        cls.sc = pyspark.SparkContext(appName=cls.__class__.__name__)
        cls.ss = sql.SparkSession(cls.sc)
    
    @classmethod
    def tearDownClass(cls):
        cls.ss.stop()
        cls.sc.stop()
