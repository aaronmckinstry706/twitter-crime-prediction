import subprocess
import unittest

import pyspark
import pyspark.sql as sql
import pyspark.sql.types as types

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
    
    def setUp(self):
        self.job_context = {
            'spark_context': self.sc,
            'spark_session': self.ss}
        
    def test_import_twitter_data(self):
        file_name = __name__ + '.csv'
        self.job_context['tweets_file_path'] = file_name
        
        input_df = self.ss.createDataFrame([
            sql.Row(id=14580, timestamp=8888888, postalCode='95823',
                    lon=34.256, lat=34.258, tweet='something something',
                    user_id=44438487, application='instagram', source='instagram?')],
            schema=types.StructType([
                types.StructField('id', types.LongType()),
                types.StructField('timestamp', types.LongType(), nullable=False),
                types.StructField('postalCode', types.StringType()),
                types.StructField('lon', types.DoubleType(), nullable=False),
                types.StructField('lat', types.DoubleType(), nullable=False),
                types.StructField('tweet', types.StringType(), nullable=False),
                types.StructField('user_id', types.LongType()),
                types.StructField('application', types.StringType()),
                types.StructField('source', types.StringType())]))
        
        try:
            input_df.coalesce(1).write.option('header', 'true').csv(file_name)
            actual = run_job.import_twitter_data(self.job_context).first().asDict()
            expected = sql.Row(timestamp=8888888, lon=34.256, lat=34.258,
                               tweet='something something').asDict()
            self.assertDictEqual(expected, actual)
        except:
            raise
        finally:
            subprocess.call(['hadoop', 'fs', '-rm', '-r', '-f', file_name])

