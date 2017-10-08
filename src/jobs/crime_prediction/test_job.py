import datetime
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
        cls.sc.setLogLevel('FATAL')
        cls.ss = sql.SparkSession(cls.sc)
    
    @classmethod
    def tearDownClass(cls):
        cls.ss.stop()
        cls.sc.stop()
    
    @unittest.skip("Skipping until run_job is finished.")
    def test_import_twitter_data(self):
        file_name = __name__ + '.csv'
        
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
            actual = run_job.import_twitter_data(self.ss, file_name).first().asDict()
            expected = sql.Row(timestamp=8888888, lon=34.256, lat=34.258,
                               tweet='something something').asDict()
            self.assertDictEqual(expected, actual)
        except:
            raise
        finally:
            subprocess.call(['hadoop', 'fs', '-rm', '-r', '-f', file_name])
    
    def test_filter_by_dates(self):
        input_tweets_schema = types.StructType([
            types.StructField('timestamp', types.LongType()),
            types.StructField('lat', types.DoubleType()),
            types.StructField('lon', types.DoubleType()),
            types.StructField('tweet', types.StringType())])
        
        output_tweets_schema = types.StructType([
            types.StructField('date', types.DateType()),
            types.StructField('lat', types.DoubleType()),
            types.StructField('lon', types.DoubleType()),
            types.StructField('tweet', types.StringType())])
        
        EPOCH = datetime.date(1970, 1, 1)
        start_date = datetime.date(2016, 3, 3)
        start_timestamp = int((start_date - EPOCH).total_seconds())
        end_date = datetime.date(2016, 3, 5)
        end_timestamp = int((end_date - EPOCH).total_seconds())
        
        input_timestamps = [
            start_timestamp  - 1,
            start_timestamp,
            start_timestamp + 1,
            end_timestamp - 1,
            end_timestamp,
            end_timestamp + 24*60*60]
        
        input_tweets_df = self.ss.createDataFrame(
            [sql.Row(timestamp=t, lat=34.89776, lon=48.3847, tweet=str(t))
             for t in input_timestamps],
            schema=input_tweets_schema)
        
        output_tweets_df = self.ss.createDataFrame(
            [sql.Row(
                date=datetime.datetime.utcfromtimestamp(t).replace(tzinfo=None).date(),
                lat=34.89776, lon=48.3847, tweet=str(t))
             for t in input_timestamps if t >= start_timestamp and t < end_timestamp],
            schema=output_tweets_schema)
        
        actual_df = run_job.filter_by_dates(
            self.ss, input_tweets_df, start_date, end_date)
        actual_list = sorted(actual_df.collect(), key=lambda d: d['tweet'])
        expected_list = sorted(output_tweets_df.collect(), key=lambda d: d['tweet'])
        
        self.assertListEqual([r.asDict() for r in expected_list],
                             [r.asDict() for r in actual_list])

