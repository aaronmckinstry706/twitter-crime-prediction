import datetime
import subprocess
import unittest

import pyspark
import pyspark.sql as sql
import pyspark.sql.types as types

from jobs.crime_prediction import run_job as run_job
from jobs.crime_prediction import grid as grid

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
    
    @unittest.skip("Skipping until run_job is finished.")
    def test_filter_by_dates(self):
        input_tweets_schema = types.StructType([
            types.StructField('timestamp', types.LongType())])
        
        output_tweets_schema = types.StructType([
            types.StructField('date', types.DateType())])
        
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
            [sql.Row(timestamp=t)
             for t in input_timestamps],
            schema=input_tweets_schema)
        
        output_tweets_df = self.ss.createDataFrame(
            [sql.Row(
                date=datetime.datetime.utcfromtimestamp(t).replace(tzinfo=None).date())
             for t in input_timestamps if t >= start_timestamp and t < end_timestamp],
            schema=output_tweets_schema)
        
        actual_df = run_job.filter_by_dates(
            self.ss, input_tweets_df, start_date, end_date)
        actual_list = sorted(actual_df.collect(), key=lambda d: d['date'])
        expected_list = sorted(output_tweets_df.collect(), key=lambda d: d['date'])
        
        self.assertListEqual(expected_list, actual_list)
    
    @unittest.skip("Skipping until run_job is finished.")
    def test_group_by_grid_square_and_tokenize(self):
        latlongrid = grid.LatLonGrid(
            lat_min = 0,
            lat_max = 10,
            lon_min = 0,
            lon_max = 10,
            lat_step = 1,
            lon_step = 1)
        
        input_tweets_schema = types.StructType(
            [types.StructField('lat', types.DoubleType()),
             types.StructField('lon', types.DoubleType()),
             types.StructField('tweet', types.StringType())])
        
        output_tokens_schema = types.StructType(
            [types.StructField('grid_square', types.IntegerType()),
             types.StructField('tokens', types.ArrayType(types.StringType()))])
        
        input_tweets_df = self.ss.createDataFrame(
            [(3.5, 3.5, '1 2 3'),
             (3.3, 3.7, '1 2 3'),
             (2.4, 5.3, '1 2 3 4 5 6')],
            schema=input_tweets_schema)
        
        expected_ouptut_tokens_df = self.ss.createDataFrame(
            [(latlongrid.grid_square_index(3.5, 3.5), ['1', '2', '3', '1', '2', '3']),
             (latlongrid.grid_square_index(2.4, 5.3), ['1', '2', '3', '4', '5', '6'])],
            schema=output_tokens_schema)
        
        actual_output_tokens_df = run_job.group_by_grid_square_and_tokenize(
            self.ss, latlongrid, input_tweets_df)
        
        def collected_sorted_by_grid_square(df):
            return sorted(df.collect(), key=lambda r: r['grid_square'])
        
        self.assertListEqual(collected_sorted_by_grid_square(expected_ouptut_tokens_df),
                             collected_sorted_by_grid_square(actual_output_tokens_df))
    
    @unittest.skip("Skipping until run_job is finished.")
    def test_load_filter_format_valid_complaints(self):
        complaints_df_schema = types.StructType([
            types.StructField('CMPLNT_NUM', types.IntegerType(),
                              nullable=False),
            types.StructField('CMPLNT_FR_DT', types.StringType()),
            types.StructField('CMPLNT_FR_TM', types.StringType()),
            types.StructField('CMPLNT_TO_DT', types.StringType()),
            types.StructField('CMPLNT_TO_TM', types.StringType()),
            types.StructField('RPT_DT', types.StringType(), nullable=False),
            types.StructField('KY_CD', types.StringType()),
            types.StructField('OFNS_DESC', types.StringType()),
            types.StructField('PD_CD', types.IntegerType()),
            types.StructField('PD_DESC', types.StringType()),
            types.StructField('CRM_ATPT_CPTD_CD', types.StringType()),
            types.StructField('LAW_CAT_CD', types.StringType()),
            types.StructField('JURIS_DESC', types.StringType()),
            types.StructField('BORO_NM', types.StringType()),
            types.StructField('ADDR_PCT_CD', types.StringType()),
            types.StructField('LOC_OF_OCCUR_DESC', types.StringType()),
            types.StructField('PREM_TYP_DESC', types.StringType()),
            types.StructField('PARKS_NM', types.StringType()),
            types.StructField('HADEVELOPT', types.StringType()),
            types.StructField('X_COORD_CD', types.FloatType()),
            types.StructField('Y_COORD_CD', types.FloatType()),
            types.StructField('Latitude', types.FloatType()),
            types.StructField('Longitude', types.FloatType()),
            types.StructField('Lat_Lon', types.StringType())])
        
        input_date_strings = [
            (None, '3/3/2015'), # Excluded.
            ('3/4/2015', None), # Included.
            ('3/5/2015', '3/5/2015'), # Included.
            ('3/6/2015', '3/7/2015')] # Excluded.
        
        LATITUDE = 34.8748
        LONGITUDE = 72.3483
        def fill_in_irrelevant_inputs(from_date, to_date):
            return (
                38, from_date, "00:30:00", to_date, "00:34:00",
                "bla", "bla", "bla", 56, "bla", "bla",
                "bla", "bla", "bla", "bla", "bla", "bla", "bla", "bla", 360.48, 330.18,
                LATITUDE, LONGITUDE, '(34.8748, 72.3483)')
        
        input_complaints_df = self.ss.createDataFrame(
            [fill_in_irrelevant_inputs(from_date, to_date)
             for from_date, to_date in input_date_strings],
            schema=complaints_df_schema)
        
        expected_output_dates = [
            datetime.date(2015, 3, 4),
            datetime.date(2015, 3, 5)]
        
        expected_df = self.ss.createDataFrame(
            [(d, LATITUDE, LONGITUDE) for d in expected_output_dates],
            schema=types.StructType(
                [types.StructField('date', types.DateType()), 
                 types.StructField('lat', types.FloatType()),
                 types.StructField('lon', types.FloatType())]))
        
        file_name = __name__ + '.csv'
        try:
            input_complaints_df.coalesce(1).write.option('header', 'true').csv(file_name)
            actual_list = (run_job.load_filter_format_valid_complaints(self.ss, file_name)
                .collect())
            expected_list = expected_df.collect()
            self.assertListEqual(expected_list, actual_list)
        except:
            raise
        finally:
            subprocess.call(['hadoop', 'fs', '-rm', '-r', '-f', file_name])

