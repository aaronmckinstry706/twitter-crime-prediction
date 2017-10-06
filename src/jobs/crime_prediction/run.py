import datetime
import logging
import operator
import os
import sys

import pyspark as pyspark
import pyspark.ml.feature as feature
import pyspark.ml.classification as classification
import pyspark.ml as ml
import pyspark.ml.clustering as clustering
import pyspark.sql as sql
import pyspark.sql.functions as functions
import pyspark.sql.types as types

import twokenize
import grid

LOGGER = logging.getLogger(__name__)
FORMATTER = logging.Formatter(
    "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s")
FILE_HANDLER = logging.FileHandler('script_log.txt')
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)
LOGGER.setLevel(logging.DEBUG)

LOGGER.info("Starting run.")

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

# --------------------------------------------------------------------------------------------------
# PART 0: Define some useful parameters that define our task. 
# --------------------------------------------------------------------------------------------------

# We are only considering data between 1 and 31 (inclusive) days prior to the prediction date.
NUM_DAYS = 31
PREDICTION_DATE = datetime.datetime(2015, 3, 3)
HISTORICAL_CUTOFF_DATE = PREDICTION_DATE - datetime.timedelta(days=31)

# We're only considering tweets and complaints within the given grid square.

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

# PART 1: Get topic distributions. 

sparkImport("twokenize", ".")
sparkImport('grid', '.')

ss = (sql.SparkSession.builder.appName("TwitterTokenizing")
                              .getOrCreate())

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
tweets_df = tweets_df.select(['timestamp', 'lon', 'lat', 'tweet'])

date_column = (tweets_df['timestamp'].cast(types.TimestampType())
                                     .cast(types.DateType()))
tweets_df = (tweets_df.withColumn('date', date_column)
                      .drop('timestamp'))

date_to_column = functions.lit(PREDICTION_DATE)
date_from_column = functions.lit(HISTORICAL_CUTOFF_DATE)
tweets_df = tweets_df.filter(
    ~(tweets_df['date'] < date_from_column)
    & (tweets_df['date'] < date_to_column))

sql_tokenize = functions.udf(
    lambda tweet: twokenize.tokenize(tweet),
    returnType=types.ArrayType(types.StringType()))
tweets_df = (tweets_df
    .withColumn('tweet_tokens', sql_tokenize(tweets_df['tweet']))
    .drop('tweet'))

# tweets_df now has Row(tweet_tokens, date, lon, lat)

# The only way to group elements and get a set of data (as far as I know) is by converting the
# DataFrame into an RDD. This is because I can't find the right operation on GroupedData in Pyspark.

row_to_gridsquare_tokens = lambda row: (
    latlongrid.grid_square_index(lat=row['lat'], lon=row['lon']),
    row['tweet_tokens'])

tokens_rdd = (tweets_df.rdd.map(row_to_gridsquare_tokens)
                           .reduceByKey(operator.concat))

tokens_df_schema = types.StructType([
    types.StructField('grid_square', types.IntegerType()),
    types.StructField('tokens', types.ArrayType(types.StringType()))
])
tokens_df = ss.createDataFrame(tokens_rdd, schema=tokens_df_schema)

hashing_tf = feature.HashingTF(
    numFeatures=(2^18)-1, inputCol='tokens', outputCol='token_frequencies')
lda = (clustering.LDA()
    .setFeaturesCol('token_frequencies')
    .setK(10)
    .setTopicDistributionCol('topic_distribution'))
topic_distribution_pipeline = ml.Pipeline(stages=[hashing_tf, lda])
lda_model = topic_distribution_pipeline.fit(tokens_df)
topic_distributions = lda_model.transform(tokens_df).select(['grid_square', 'topic_distribution'])

# --------------------------------------------------------------------------------------------------
# PART 2: Get complaint counts per (grid square, date). 
# --------------------------------------------------------------------------------------------------

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
        return datetime.datetime.strptime(s, '%m/%d/%Y')

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
# Columns are now 'date', 'lat', and 'lon'.

# Compute grid square for each crime. 

def grid_square_from_lat_lon(lat, lon):
    return latlongrid.grid_square_index(lat=lat, lon=lon)
grid_square_from_lat_lon_udf = functions.udf(
    grid_square_from_lat_lon, returnType=types.IntegerType())

complaints_df = (complaints_df
    .withColumn('grid_square',
                grid_square_from_lat_lon_udf(complaints_df['lat'], complaints_df['lon']))
    .select('date', 'grid_square'))

# Now count by (GridSquare, Date).

complaint_counts_df = (complaints_df
    .groupBy(complaints_df['grid_square'], complaints_df['date'])
    .count()
    .withColumnRenamed('count', 'complaint_count'))
complaint_counts_df = (complaint_counts_df
    .withColumn(
        'complaint_count',
        complaint_counts_df['complaint_count'].cast(types.DoubleType())))
count_binarizer = feature.Binarizer(
    threshold=0, inputCol='complaint_count', outputCol='binary_complaint_count')
complaint_counts_df = count_binarizer.transform(complaint_counts_df)
complaint_counts_df = (complaint_counts_df
    .drop('complaint_counts')
    .withColumnRenamed('binary_complaint_counts', 'complaint_counts'))
# Columns are now 'date', 'grid_square', 'complaint_count', 'binary_complaint_count'. 

# Filter for complaints occurring within the date range..

past_complaint_counts_df = complaint_counts_df.filter(
    (complaint_counts_df['date'] < date_to_column)
    & (complaint_counts_df['date'] >= date_from_column))

current_complaint_counts_df = complaint_counts_df.filter(
    complaint_counts_df['date'] == date_to_column)

# --------------------------------------------------------------------------------------------------
# PART 3: Defining the data matrix.
# --------------------------------------------------------------------------------------------------

# Complaint count dataframes only have entries for nonzero counts. Fill in the nonzero entries.

all_dates_squares_df = ss.createDataFrame(
    [(gridSquare, PREDICTION_DATE - datetime.timedelta(days=i))
     for gridSquare in range(-1, latlongrid.grid_size)
     for i in range(1, 1 + NUM_DAYS)],
    schema=types.StructType([
        types.StructField('grid_square', types.IntegerType()),
        types.StructField('date', types.DateType())]))

all_squares_df = ss.createDataFrame(
    [(gridSquare,) for gridSquare in range(-1, latlongrid.grid_size)],
    schema=types.StructType([
        types.StructField('grid_square', types.IntegerType())]))

past_complaint_counts_df = past_complaint_counts_df.join(
    all_dates_squares_df,
    on=['grid_square', 'date'],
    how='right_outer')

past_complaint_counts_df = past_complaint_counts_df.fillna({'complaint_count': 0})

current_complaint_counts_df = current_complaint_counts_df.join(
    all_squares_df,
    on='grid_square',
    how='right_outer')

current_complaint_counts_df = (current_complaint_counts_df
    .fillna({'complaint_count': 0})
    .withColumn('date',
                functions.when(current_complaint_counts_df['date'].isNull(), PREDICTION_DATE)
                         .otherwise(current_complaint_counts_df['date'])))

# Do a left outer join on topic_distributions and past_complaint_counts_df to get our data matrix. 

data_matrix = topic_distributions.join(
    past_complaint_counts_df,
    on='grid_square',
    how='inner')
# So far, data_matrix contains Row(date, grid_square, topic_distributions, complaint_count). 

# Get weekday from date.

get_weekday_udf = functions.udf(lambda d: d.weekday(), returnType=types.IntegerType())
data_matrix = data_matrix.withColumn('weekday', get_weekday_udf(data_matrix['date']))

# Assemble the feature vectors.

weekday_one_hot_encoder = feature.OneHotEncoder(inputCol='weekday', outputCol='weekday_vector')
feature_vector_assembler = feature.VectorAssembler(
    inputCols=['weekday_vector', 'topic_distribution'], outputCol='final_feature_vector')
feature_assembly_pipeline = (
    ml.Pipeline(stages=[weekday_one_hot_encoder, feature_vector_assembler]).fit(data_matrix))

data_matrix = (feature_assembly_pipeline.transform(data_matrix)
    .select('date', 'grid_square', 'final_feature_vector', 'complaint_count'))

LOGGER.debug(str(data_matrix.count()) + " rows like " + str(data_matrix.take(1)))

#logistic_regression = classification.LogisticRegression(
#    maxIter=10, regParam=0.3, elasticNetParam=0.8,
#    featuresCol='final_feature_vector', labelCol='complaint_count',
#    probabilityCol='predicted_probability')
#logistic_model = logistic_regression.fit(data_matrix)

#LOGGER.info(
#    "coefficients: " + str(logistic_model.coefficientMatrix) + ", intercept: " + str(logistic_model.interceptVector))

prediction_data_matrix = topic_distributions.join(
    current_complaint_counts_df,
    on='grid_square',
    how='inner')
prediction_data_matrix = (prediction_data_matrix
    .withColumn('weekday', get_weekday_udf(prediction_data_matrix['date']))
    .select('weekday', 'grid_square', 'date', 'topic_distribution', 'complaint_count'))
prediction_data_matrix = (feature_assembly_pipeline.transform(prediction_data_matrix)
    .select('grid_square', 'date', 'final_feature_vector', 'complaint_count'))

LOGGER.debug(str(prediction_data_matrix.count()) + " rows like "
             + str(prediction_data_matrix.take(1)))
exit(0)

#predicted_complaint_counts = (logistic_model.transform(prediction_data_matrix)
#    .select('grid_square', 'complaint_count', 'predicted_probability')
#    .collect())
#
#LOGGER.debug(str(predicted_complaint_counts.count()) + " rows like "
#             + str(predicted_complaint_counts.take(1)) + ".")
#exit(0)

