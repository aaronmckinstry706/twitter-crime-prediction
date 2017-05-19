import datetime
import logging
import operator
import sys

import pyspark
import pyspark.ml.clustering as clustering
import pyspark.ml.feature as feature
import pyspark.sql as sql
import sqlite3

import utilities

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    logger.info(str(datetime.datetime.now()))
    
    logger.info('Processing tweets...')
    
    spark_context = pyspark.SparkContext()
    tweets_by_day = spark_context.textFile('tweets.csv') \
        .filter(utilities.format_is_correct) \
        .map(utilities.split_record) \
        .filter(lambda record: record[utilities.field_index['timestamp']] > 1463254639 - 1 * 2592000) \
        .map(utilities.get_tweet_modifier(utilities.remove_url)) \
        .map(utilities.get_tweet_modifier(utilities.remove_unicode)) \
        .map(utilities.get_tweet_modifier(utilities.remove_apostrophe_in_contractions)) \
        .map(utilities.get_tweet_modifier(utilities.keep_only_alphanumeric)) \
        .map(utilities.get_tweet_modifier(utilities.strip_excessive_whitespace)) \
        .map(lambda record: (record[utilities.field_index['timestamp']] / (60 * 60 * 24), record[utilities.field_index['tweet']])) \
        .saveAsTextFile('tweets_by_day_text')
    
    logger.info("finished")

