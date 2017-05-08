import datetime
import logging

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
    tweets_csv = spark_context.textFile('tweets.csv') \
        .filter(utilities.format_is_correct) \
        .map(utilities.remove_url) \
        .map(utilities.remove_unicode) \
        .map(utilities.keep_only_alphanumeric)
    logger.info('first tweet entry: ' + str(tweets_csv.first()))

    logger.info("finished")
