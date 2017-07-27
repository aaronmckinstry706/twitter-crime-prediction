import datetime
import logging
import operator
import sys

import pyspark
import pyspark.ml.clustering as clustering
import pyspark.ml.feature as feature
import pyspark.sql as sql
import sqlite3

import preprocessing as pp
import grid
import clargs

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.FileHandler('script_log.txt'))
    logger.setLevel(logging.INFO)

    logger.info(str(datetime.datetime.now()))
    
    command_line_args = clargs.process_argv(sys.argv)
    if not command_line_args:
        print("Usage: python run.py <year> <month> <day> \n(year/month/day are all integers)")
        exit()
    (year, month, day) = command_line_args
    prediction_timestamp = (datetime.datetime(year, month, day) - datetime.datetime(1970, 1, 1)) \
        .total_seconds()
    tweet_history_cutoff = prediction_timestamp - 31*24*60*60

    logger.info('Processing tweets...')
    
    grid_boundaries = grid.read_grid_csv('grid_bounds.csv')

    spark_context = pyspark.SparkContext()
    tweet_records = spark_context.textFile('tweets.csv') \
        .filter(pp.tweet_format_is_correct) \
        .map(pp.split_tweet_record) \
        .filter(lambda record: record[pp.tweet_field_index['timestamp']] > tweet_history_cutoff \
                           and record[pp.tweet_field_index['timestamp']] < prediction_timestamp)
    num_tweets = tweet_records.count()
    logger.info('number of tweets: ' + str(num_tweets))
    tweet_grid = tweet_records \
        .map(pp.get_tweet_modifier(pp.remove_url)) \
        .map(pp.get_tweet_modifier(pp.remove_unicode)) \
        .map(pp.get_tweet_modifier(pp.remove_apostrophe_in_contractions)) \
        .map(pp.get_tweet_modifier(pp.keep_only_alphanumeric)) \
        .map(pp.get_tweet_modifier(pp.strip_excessive_whitespace)) \
        .map(lambda record: (pp.get_grid_index(grid_boundaries, record, 'tweet'),
                             record[pp.tweet_field_index['tweet']])) \
        .map(lambda pair: (pair[0], pair[1].split(' '))) \
        .reduceByKey(lambda a,b: a + b)
    
    spark_session = sql.SparkSession.builder.appName("twitter-crime-prediction").getOrCreate()
    tweet_grid_dataframe = spark_session.createDataFrame(tweet_grid, schema=["grid_id", "words"])
    count_vectorizer = feature.CountVectorizer(inputCol="words", outputCol="features")
    vectorizer_model = count_vectorizer.fit(tweet_grid_dataframe)
    tweet_grid_dataframe = vectorizer_model.transform(tweet_grid_dataframe)
    
    lda = clustering.LDA().setFeaturesCol("features").setK(10).setTopicDistributionCol("topic_distributions")
    lda_model = lda.fit(tweet_grid_dataframe)
    topicDistributions = lda_model.transform(tweet_grid_dataframe)
    
    logger.info('first dataframe entry: ' + str(topicDistributions.first().asDict()))
    
    complaints_df = spark_session.read.load(
        "crime_complaints_with_header.csv",
        format="com.databricks.spark.csv",
        header="true",
        inferSchema="true")
    complaints_rdd = complaints_df.rdd.map(list).filter(pp.)
    
    
    logger.info("finished")

