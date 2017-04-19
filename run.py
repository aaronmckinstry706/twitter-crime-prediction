import datetime
import logging

import pyspark
import sqlite3

def preprocess_tweet_entry(t):
    t.replace('\0', ' ')
    t.replace('\1', ' ')
    t.replace('\2', ' ')
    return t.split(',')

def assign_to_grid(x):
    return (random.randint(1, 100), x)

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.FileHandler(
        '/home/gimlisonofgloin1/twitter-crime-prediction/script_log.txt'))
    logger.setLevel(logging.INFO)

    logger.info(str(datetime.datetime.now()))

    logger.info('Processing tweets...')

    spark_context = pyspark.SparkContext()
    tweets_csv = spark_context.textFile(
        '/home/gimlisonofgloin1/big-data-crime-nyu/OUT/tweets.csv') \
        .map(assign_to_grid)
    logger.info('first tweet entry: ' + str(tweets_csv.first()))

    logger.info("finished")
