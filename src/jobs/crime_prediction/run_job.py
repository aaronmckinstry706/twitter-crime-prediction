import pyspark.sql as sql
import pyspark.sql.types as types

def import_twitter_data(job_context):
    """Imports the twitter data as DataFrame, drops all columns except
    [timestamp, lon, lat, tweet], and returns resulting DataFrame."""
    
    required_keys = ['tweets_file_path', 'spark_session']
    if not all([(key in job_context) for key in required_keys]):
        missing_keys = [k for k in required_keys if k not in job_context]
        raise ValueError(
            'job_context {} is missing keys {}.'.format(job_context, missing_keys))
    
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
    
    tweets_df = job_context['spark_session'].read.csv(
        job_context['tweets_file_path'],
        escape='"',
        header='true',
        schema=tweets_schema,
        mode='DROPMALFORMED')
    
    tweets_df = tweets_df.select(['timestamp', 'lon', 'lat', 'tweet'])
    return tweets_df

def run(sc, args):
    job_context = {}
    job_context['spark_context'] = sc
    job_context['spark_session'] = sql.SparkSession(sc)
    job_context['tweets_file_path'] = 'tweets2.csv'
