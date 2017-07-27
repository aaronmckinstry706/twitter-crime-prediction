import datetime
import logging
import operator
import sys

import pyspark
import pyspark.ml.clustering as clustering
import pyspark.ml.feature as feature
import pyspark.sql as sql
import sqlite3

import preprocessing

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    logger.info(str(datetime.datetime.now()))
    
    logger.info('Processing tweets...')
    
    spark_context = pyspark.SparkContext()
    tweets_by_day = spark_context.textFile('tweets.csv') \
        .filter(preprocessing.tweet_format_is_correct) \
        .map(preprocessing.split_tweet_record) \
        .filter(lambda record: record[preprocessing.tweet_field_index['timestamp']] > 1463254639 - 1 * 2592000) \
        .map(preprocessing.get_tweet_modifier(preprocessing.remove_url)) \
        .map(preprocessing.get_tweet_modifier(preprocessing.remove_unicode)) \
        .map(preprocessing.get_tweet_modifier(preprocessing.remove_apostrophe_in_contractions)) \
        .map(preprocessing.get_tweet_modifier(preprocessing.keep_only_alphanumeric)) \
        .map(preprocessing.get_tweet_modifier(preprocessing.strip_excessive_whitespace)) \
        .map(lambda record: str(record[preprocessing.tweet_field_index['timestamp']] / (60 * 60 * 24)) + "," + record[preprocessing.tweet_field_index['tweet']]) \
        .saveAsTextFile('tweets_by_day_text')
    
    logger.info("finished")

