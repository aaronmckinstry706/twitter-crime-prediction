import argparse
import datetime
import operator

import pyspark.ml as ml
import pyspark.ml.feature as feature
import pyspark.ml.clustering as clustering
import pyspark.sql as sql
import pyspark.sql.functions as functions
import pyspark.sql.types as types

from jobs.crime_prediction import grid as grid
from jobs.crime_prediction import twokenize as twokenize

def import_twitter_data(spark_session, tweets_file_path):
    """Imports the twitter data and returns resulting DataFrame.
    
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
    """Replaces the 'timestamp' column with 'date' column of type DateTime, then returns a
    DataFrame containing only entries with a 'date' between the start (inclusive) and end
    (exclusive) dates.
    
    The timestamps, and the resulting dates, are both assumed to be in UTC time. However,
    the resulting dates are naive (i.e., not timezone-aware) datetime.date objects.
    
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

def group_by_grid_square_and_tokenize(spark_session, latlongrid, tweets_df):
    """Calculates the grid square id from 'lat' and 'lon' columns in tweets_df, and then
    groups the tweets by grid square. Tweets are tokenized. Returned dataframe has
    columns ['grid_square', 'tokens'], where 'tokens' is a list of all tokens from every
    tweet within an entry's 'grid_square'.
    
    Args:
        spark_session    --    An active SparkSession.
        latlongrid    --    A LatLonGrid object.
        tweets_df    --    A dataframe with columns ['lat', 'lon', and 'tweet'] of types
                           [DoubleType, DoubleType, StringType]."""
    
    sql_tokenize = functions.udf(
        lambda tweet: twokenize.tokenize(tweet),
        returnType=types.ArrayType(types.StringType()))
    tweets_df = (tweets_df
        .withColumn('tweet_tokens', sql_tokenize(tweets_df['tweet']))
        .drop('tweet'))
    
    row_to_gridsquare_tokens = lambda row: (
        latlongrid.grid_square_index(lat=row['lat'], lon=row['lon']),
        row['tweet_tokens'])
    
    tokens_rdd = (tweets_df.rdd.map(row_to_gridsquare_tokens)
                               .reduceByKey(operator.concat))
    
    tokens_df_schema = types.StructType([
        types.StructField('grid_square', types.IntegerType()),
        types.StructField('tokens', types.ArrayType(types.StringType()))
    ])
    tokens_df = spark_session.createDataFrame(tokens_rdd, schema=tokens_df_schema)
    
    return tokens_df

def load_filter_format_valid_complaints(spark_session, complaints_file_path):
    """Loads the complaints and returns the resulting DataFrame. Only crimes which occur
    during a single day are included. The date columns, originally strings, are converted
    to datetime.date objects. The returned columns are ['date', 'lat', 'lon']"""
    
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
    
    complaints_df = spark_session.read.csv(
        complaints_file_path,
        header=True,
        schema=complaints_df_schema)
    
    complaints_df = (complaints_df
        .select(['CMPLNT_FR_DT', 'CMPLNT_TO_DT', 'Latitude', 'Longitude'])
        .withColumnRenamed('CMPLNT_FR_DT', 'from_date_string')
        .withColumnRenamed('CMPLNT_TO_DT', 'to_date_string')
        .withColumnRenamed('Latitude', 'lat')
        .withColumnRenamed('Longitude', 'lon'))
    
    # Filter to find the complaints which have an exact date of occurrence
    # or which have a start and end date.
    
    complaints_df = complaints_df.filter(~complaints_df['from_date_string'].isNull())
    
    # Now get the actual column dates.
    
    def string_to_date(s):
        if s == None:
            return None
        else:
            return datetime.datetime.strptime(s, '%m/%d/%Y').date()
    
    string_to_date_udf = functions.udf(string_to_date, types.DateType())
    
    complaints_df = (complaints_df
        .withColumn('from_date', string_to_date_udf(complaints_df['from_date_string']))
        .withColumn('to_date', string_to_date_udf(complaints_df['to_date_string']))
        .select(['from_date', 'to_date', 'lat', 'lon']))
    
    # Now filter for complaints which occur on one day only. 
    
    complaints_df = (complaints_df
        .filter(complaints_df['to_date'].isNull()
                | (complaints_df['to_date'] == complaints_df['from_date']))
        .withColumnRenamed('from_date', 'date'))
    
    return complaints_df.select('date', 'lat', 'lon')

def run(sc, args):
    sc.setLogLevel('FATAL')
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('year', help='Year of prediction, in format YYYY.', type=int)
    arg_parser.add_argument('month', help='Month of prediction, in format MM.', type=int)
    arg_parser.add_argument('day', help='Day of prediction, in format DD.', type=int)
    args = arg_parser.parse_args(args)
    
    ss = sql.SparkSession(sc)
    
    latlongrid = grid.LatLonGrid(
        lat_min=40.488320,
        lat_max=40.957189,
        lon_min=-74.290739,
        lon_max=-73.635679,
        lat_step=grid.get_lon_delta(1000, (40.957189 - 40.488320)/2.0),
        lon_step=grid.get_lat_delta(1000))
    
    tweets_df = import_twitter_data(ss, 'tweets2.csv')
    
    prediction_date = datetime.date(args.year, args.month, args.day)
    NUM_DAYS_IN_HISTORY = 31
    history_cutoff = prediction_date - datetime.timedelta(days=NUM_DAYS_IN_HISTORY)
    filtered_tweets_df = filter_by_dates(ss, tweets_df, history_cutoff, prediction_date)
    
    tokens_df = group_by_grid_square_and_tokenize(ss, latlongrid, filtered_tweets_df)
    
    hashing_tf = feature.HashingTF(
        numFeatures=(2^18)-1, inputCol='tokens', outputCol='token_frequencies')
    lda = (clustering.LDA()
        .setFeaturesCol('token_frequencies')
        .setK(10)
        .setTopicDistributionCol('topic_distribution'))
    topic_distribution_pipeline = ml.Pipeline(stages=[hashing_tf, lda])
    lda_model = topic_distribution_pipeline.fit(tokens_df)
    topic_distributions = (lda_model.transform(tokens_df)
                           .select(['grid_square', 'topic_distribution']))
    
    complaints_df = load_filter_format_valid_complaints(
        ss, 'crime_complaints_with_header.csv')
    
    complaints_df.show()
    
    #...

