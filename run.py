import datetime
import logging
import operator
import os
import sys

import pyspark as pyspark
import pyspark.ml.feature as feature
import pyspark.ml as ml
import pyspark.ml.clustering as clustering
import pyspark.sql as sql
import pyspark.sql.functions as functions
import pyspark.sql.types as types

import twokenize
import grid

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler(sys.stderr))
LOGGER.addHandler(logging.FileHandler('script_log.txt'))
LOGGER.setLevel(logging.DEBUG)

sc = pyspark.SparkContext()

# From https://stackoverflow.com/a/36218558 .
def sparkImport(module_name, module_directory):
    """
    Convenience function. 
    
    Tells the SparkContext sc (must already exist) to load
    module module_name on every computational node before
    executing an RDD. 
    
    Args:
        module_name: the name of the module, without ".py". 
        module_directory: the path, absolute or relative, to
                          the directory containing module
                          module_Name. 
    
    Returns: none. 
    """
    module_path = os.path.abspath(
        module_directory + "/" + module_name + ".py")
    sc.addPyFile(module_path)

sparkImport("twokenize", ".")
sparkImport('grid', '.')

ss = sql.SparkSession.builder.appName("TwitterTokenizing")\
                             .getOrCreate()

tweets_schema = types.StructType([
  types.StructField('id', types.LongType()),
  types.StructField('timestamp', types.LongType()),
  types.StructField('postalCode', types.StringType()),
  types.StructField('lon', types.DoubleType()),
  types.StructField('lat', types.DoubleType()),
  types.StructField('tweet', types.StringType()),
  types.StructField('user_id', types.LongType()),
  types.StructField('application', types.StringType()),
  types.StructField('source', types.StringType())
])
tweets_df = ss.read.csv('tweets2.csv',
                         escape='"',
                         header='true',
                         schema=tweets_schema,
                         mode='DROPMALFORMED')
tweets_df = tweets_df.drop('id') \
                     .drop('postalCode') \
                     .drop('user_id') \
                     .drop('application') \
                     .drop('source')

date_column = tweets_df['timestamp'].cast(types.TimestampType()) \
                                    .cast(types.DateType())
tweets_df = tweets_df.withColumn('date', date_column) \
                     .drop('timestamp')

date_to_column = functions.lit(datetime.datetime(2016, 3, 3))
date_from_column = functions.lit(functions.date_sub(date_to_column, 31))
tweets_df = tweets_df.filter(
    ~(tweets_df.date < date_from_column)
    & (tweets_df.date < date_to_column))

sql_tokenize = functions.udf(
    lambda tweet: twokenize.tokenize(tweet),
    returnType=types.ArrayType(types.StringType()))
tweets_df = tweets_df \
    .withColumn("tweet_tokens", sql_tokenize(tweets_df.tweet)) \
    .drop('tweet')

# Southwest corner of New York:
# lat = 40.488320, lon = -74.290739
# Northeast corner of New York:
# lat = 40.957189, lon = -73.635679

latlongrid = grid.LatLonGrid(
    lat_min=40.488320,
    lat_max=40.957189,
    lon_min=-74.290739,
    lon_max=-73.635679,
    lat_step=grid.get_lon_delta(1000, (40.957189 - 40.488320)/2.0),
    lon_step=grid.get_lat_delta(1000))

# The only way to group elements and get a set of data (as far as I know) is by converting the DataFrame into an RDD. 

row_to_gridsquare_tokens = lambda row: (
    latlongrid.grid_square_index(lat=row['lat'], lon=row['lon']),
    row['tweet_tokens'])

tokens_rdd = tweets_df.rdd.map(row_to_gridsquare_tokens) \
                          .reduceByKey(operator.concat)

tokens_df_schema = types.StructType([
    types.StructField('grid_square', types.IntegerType()),
    types.StructField('tokens', types.ArrayType(types.StringType()))
])
tokens_df = ss.createDataFrame(tokens_rdd, schema=tokens_df_schema)

hashing_tf = feature.HashingTF(numFeatures=(2^18)-1, inputCol='tokens', outputCol='token_frequencies')
lda = clustering.LDA().setFeaturesCol('token_frequencies').setK(10).setTopicDistributionCol('topic_distributions')
pipeline = ml.Pipeline(stages=[hashing_tf, lda])
lda_model = pipeline.fit(tokens_df)
topic_distributions = lda_model.transform(tokens_df).drop('tokens').drop('token_frequencies')

LOGGER.debug(str(topic_distributions.count()) + " entries like " + str(topic_distributions.take(1)))

