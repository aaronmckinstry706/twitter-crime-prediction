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

    logger.info('Processing tweets...')

    spark_context = pyspark.SparkContext()
    tweets = spark_context.textFile('tweets.csv') \
        .filter(utilities.format_is_correct) \
        .map(utilities.split_record) \
        .map(utilities.get_tweet_modifier(utilities.remove_url)) \
        .map(utilities.get_tweet_modifier(utilities.remove_unicode)) \
        .map(utilities.get_tweet_modifier(utilities.remove_apostrophe_in_contractions)) \
        .map(utilities.get_tweet_modifier(utilities.keep_only_alphanumeric)) \
        .map(utilities.get_tweet_modifier(utilities.strip_excessive_whitespace)) \
        .map(lambda record: (utilities.get_grid_index(grid_boundaries, record),
                             record[utilities.field_index['tweet']])) \
        .reduceByKey(operator.add)
    logger.info('first tweet entry: ' + str(tweets.first()))

    logger.info("finished")
