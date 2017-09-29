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

# PART 1: Get topic distributions. 

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

# PART 2: Get complaint counts per (grid square, date). 

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

complaints_df = ss.read.csv(
    "crime_complaints_with_header.csv",
    header=True,
    schema=complaints_df_schema)

complaints_df = complaints_df \
    .drop('CMPLNT_NUM') \
    .drop('CMPLNT_FR_TM') \
    .drop('CMPLNT_TO_TM') \
    .drop('RPT_DT') \
    .drop('KY_CD') \
    .drop('OFNS_DESC') \
    .drop('PD_CD') \
    .drop('PD_DESC') \
    .drop('CRM_ATPT_CPTD_CD') \
    .drop('LAW_CAT_CD') \
    .drop('JURIS_DESC') \
    .drop('BORO_NM') \
    .drop('ADDR_PCT_CD') \
    .drop('LOC_OF_OCCUR_DESC') \
    .drop('PREM_TYP_DESC') \
    .drop('PARKS_NM') \
    .drop('HADEVELOPT') \
    .drop('X_COORD_CD') \
    .drop('Y_COORD_CD') \
    .drop('Lat_Lon')

# Filter to find the complaints which have an exact date of occurrence
# or which have a start and end date.

complaints_df = complaints_df \
    .filter(~complaints_df.CMPLNT_FR_DT.isNull())

def string_to_date(s):
    if s == None:
        return None
    else:
        return datetime.datetime.strptime(s, '%m/%d/%Y')

string_to_date_udf = functions.udf(string_to_date, types.DateType())

# Now get the actual column dates.

complaints_df = complaints_df \
    .withColumn(
        'FR_DT',
        string_to_date_udf(complaints_df.CMPLNT_FR_DT)) \
    .drop('CMPLNT_FR_DT') \
    .withColumn('TO_DT',
                string_to_date_udf(complaints_df.CMPLNT_TO_DT)) \
    .drop('CMPLNT_TO_DT')

# Now filter for complaints which occur on one day only. 

complaints_df = complaints_df \
    .filter(complaints_df.TO_DT.isNull() | (complaints_df.TO_DT == complaints_df.FR_DT)) \
    .drop(complaints_df.TO_DT) \
    .withColumnRenamed('FR_DT', 'Date')

# Filter for complaints occurring within the date range..

complaints_df = complaints_df.filter(
    (complaints_df.Date < date_to_column) & (complaints_df.Date >= date_from_column))

# Compute grid square for each crime. 

def grid_square_from_lat_lon(lat, lon):
    return latlongrid.grid_square_index(lat=lat, lon=lon)
grid_square_from_lat_lon_udf = functions.udf(grid_square_from_lat_lon, returnType=types.IntegerType())

grid_square_column = grid_square_from_lat_lon_udf(complaints_df.Latitude, complaints_df.Longitude)

complaints_df = complaints_df \
    .withColumn('GridSquare', grid_square_column) \
    .drop('Latitude') \
    .drop('Longitude')

# Now count by (GridSquare, Date).

complaints_df = complaints_df \
    .groupBy(complaints_df.GridSquare, complaints_df.Date) \
    .count()

# PART 3: Defining the data matrix.

data_matrix = topic_distributions.join(
    complaints_df,
    on=(complaints_df.GridSquare == topic_distributions.grid_square),
    how='left_outer')
data_matrix = data_matrix \
    .drop('GridSquare') \
    .withColumnRenamed('Date', 'date')
# Now we should have our data matrix: Row(date, grid_square, topic_distribution). 

# Last part: fill Null crime count values with 0. 

data_matrix = data_matrix.fillna({'count': 0})

LOGGER.debug(data_matrix.take(10))
LOGGER.debug(data_matrix.count())
