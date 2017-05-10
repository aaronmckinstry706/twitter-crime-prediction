import datetime
import logging
import operator

import pyspark
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

    spark_context = pyspark.SparkContext()
    tweets = spark_context.textFile('tweets.csv') \
        .filter(utilities.format_is_correct) \
        .map(utilities.split_record) \
        .filter(lambda record: record[utilities.field_index['timestamp']] > tweet_history_cutoff \
                    and record[utilities.field_index['timestamp']] < prediction_timestamp) \
        .map(utilities.get_tweet_modifier(utilities.remove_url)) \
        .map(utilities.get_tweet_modifier(utilities.remove_unicode)) \
        .map(utilities.get_tweet_modifier(utilities.remove_apostrophe_in_contractions)) \
        .map(utilities.get_tweet_modifier(utilities.keep_only_alphanumeric)) \
        .map(utilities.get_tweet_modifier(utilities.strip_excessive_whitespace)) \
        .map(lambda record: (utilities.get_grid_index(grid_boundaries, record),
                             record[utilities.field_index['tweet']])) \
        .reduceByKey(lambda (a,b): a + ' ' + b)
    logger.info('first tweet entry: ' + str(tweets.first()))

    logger.info("finished")
