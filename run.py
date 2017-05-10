import datetime
import logging
import operator
import sys

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
    
    command_line_args = utilities.process_argv(sys.argv)
    if not command_line_args:
        print("Usage: python run.py <year> <month> <day> \n(year/month/day are all integers)")
        exit()
    (year, month, day) = command_line_args
    prediction_timestamp = (datetime.datetime(year, month, day) - datetime.datetime(1970, 1, 1)) \
        .total_seconds()
    tweet_history_cutoff = prediction_timestamp - 31*24*60*60

    logger.info('Processing tweets...')
    
    grid_boundaries = utilities.read_grid_csv('grid_bounds.csv')

    spark_context = pyspark.SparkContext()
    tweet_records = spark_context.textFile('tweets.csv') \
        .filter(utilities.format_is_correct) \
        .map(utilities.split_record) \
        .filter(lambda record: record[utilities.field_index['timestamp']] > tweet_history_cutoff \
                           and record[utilities.field_index['timestamp']] < prediction_timestamp)
    num_tweets = tweet_records.count()
    logger.info('number of tweets: ' + str(num_tweets))
    tweet_grid = tweet_records \
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
