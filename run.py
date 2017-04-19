import datetime
import logging
import random

import pyspark
import sqlite3

def preprocess_tweet_entry(t):
    t.replace('\0', ' ')
    t.replace('\1', ' ')
    t.replace('\2', ' ')
    return t.split(',')

def assign_to_document(x):
    return (random.randint(1, 100), x)

def get_tweet(tweet_entry):
    TWEET_INDEX = 5
    return tweet_entry[TWEET_INDEX][1:-1]

def split_tweet_into_words(tweet):
    return [word for word in tweet.split()]

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.FileHandler(
        'script_log.txt'))
    logger.setLevel(logging.INFO)

    logger.info(str(datetime.datetime.now()))

    logger.info('Processing tweets...')

    spark_context = pyspark.SparkContext()
    tweets_csv = spark_context.textFile('tweets.csv') \
        .map(preprocess_tweet_entry) \
        .map(get_tweet) \
        .flatMap(split_tweet_into_words)
    logger.info('first tweet entry: ' + str(tweets_csv.first()))

    logger.info("finished")
