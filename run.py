import datetime
import logging
import operator

import pyspark
import pyspark.ml.feature as feature
import pyspark.sql as sql
import sqlite3

import utilities

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.FileHandler('script_log.txt'))
    logger.setLevel(logging.INFO)

    logger.info(str(datetime.datetime.now()))

    logger.info('Processing tweets...')
    
    grid_boundaries = utilities.read_grid_csv('grid_bounds.csv')

    spark_context = pyspark.SparkContext()
    tweet_grid = spark_context.textFile('tweets.csv') \
        .filter(utilities.format_is_correct) \
        .map(utilities.split_record) \
        .filter(lambda record: record[utilities.field_index['timestamp']] > 1464783730) \
        .map(utilities.get_tweet_modifier(utilities.remove_url)) \
        .map(utilities.get_tweet_modifier(utilities.remove_unicode)) \
        .map(utilities.get_tweet_modifier(utilities.remove_apostrophe_in_contractions)) \
        .map(utilities.get_tweet_modifier(utilities.keep_only_alphanumeric)) \
        .map(utilities.get_tweet_modifier(utilities.strip_excessive_whitespace)) \
        .map(lambda record: (utilities.get_grid_index(grid_boundaries, record),
                             record[utilities.field_index['tweet']])) \
        .map(lambda pair: (pair[0], pair[1].split(' '))) \
        .reduceByKey(lambda a,b: a + b)
    
    spark_session = sql.SparkSession.builder.appName("twitter-crime-prediction").getOrCreate()
    tweet_grid_dataframe = spark_session.createDataFrame(tweet_grid, schema=["grid_id", "words"])
    count_vectorizer = feature.CountVectorizer(inputCol="words", outputCol="features")
    vectorizer_model = count_vectorizer.fit(tweet_grid_dataframe)
    tweet_grid_dataframe = vectorizer_model.transform(tweet_grid_dataframe)
    
    logger.info('first dataframe entry: ' + str(tweet_grid_dataframe.first().asDict()))
    
    logger.info("finished")
