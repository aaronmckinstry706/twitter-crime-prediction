import datetime

import pyspark.sql as sql
import pyspark.sql.functions as functions
import pyspark.sql.types as types

def import_twitter_data(spark_session, tweets_file_path):
    """Imports the twitter data and returns resulting DataFrame, with columns
    [timestamp, lon, lat, tweet].
    
    Args:
        spark_session    --    An active SparkSession.
        tweets_file_path    --    A file path.
    """
    
    tweets_schema = types.StructType([
      types.StructField('id', types.LongType()),
      types.StructField('timestamp', types.LongType(), nullable=False),
      types.StructField('postalCode', types.StringType()),
      types.StructField('lon', types.DoubleType(), nullable=False),
      types.StructField('lat', types.DoubleType(), nullable=False),
      types.StructField('tweet', types.StringType(), nullable=False),
      types.StructField('user_id', types.LongType()),
      types.StructField('application', types.StringType()),
      types.StructField('source', types.StringType())])
    
    tweets_df = spark_session.read.csv(
        tweets_file_path,
        escape='"',
        header='true',
        schema=tweets_schema,
        mode='DROPMALFORMED')
    
    tweets_df = tweets_df.select(['timestamp', 'lon', 'lat', 'tweet'])
    return tweets_df

def filter_by_dates(spark_session, tweets_df, start_date_inclusive, end_date_exclusive):
    """Converts the 'timestamp' column to 'date' column of type DateTime, then returns a
    DataFrame containing only tweets which occurred between the start (inclusive) and end
    (exclusive) dates; columns are ['date', 'lat', 'lon', 'tweet'].
    
    Args:
        spark_session    --    An active SparkSession.
        tweets_df    --    A DataFrame with a 'timestamp' column of type LongType.
        start_date_inclusive    --    A datetime.date object.
        end_date_exclusive    --    A datetime.date object.
    """
    
    def timestamp_to_date(t):
        # The date is calculated.
        return datetime.datetime.utcfromtimestamp(t).replace(tzinfo=None).date()
    timestamp_to_date_udf = functions.udf(
        timestamp_to_date, returnType=types.DateType())
    
    date_column = timestamp_to_date_udf(tweets_df['timestamp'])
    tweets_df = (tweets_df.withColumn('date', date_column)
                          .drop('timestamp'))
    
    date_to_column = functions.lit(end_date_exclusive)
    date_from_column = functions.lit(start_date_inclusive)
    
    tweets_df = tweets_df.filter(
        (tweets_df['date'] >= date_from_column)
        & (tweets_df['date'] < date_to_column))
    
    return tweets_df

def run(sc, args):
    ss = sql.SparkSession(sc)

