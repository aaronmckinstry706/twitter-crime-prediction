import pyspark.sql as sql

ss = sql.SparkSession.builder.appName("TwitterTokenizing")\
                             .getOrCreate()

import pyspark.sql.types as types

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

print('Dataframe columns:')
print(tweets_df.columns)
print('Sample row:')
print(tweets_df.take(1))
print('Number of tweets:')
print(tweets_df.count())
