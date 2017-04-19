import datetime
import logging

import pyspark
import sqlite3

def get_grid_block_boundaries(south_west_coordinates, north_east_coordinates,
                              n):
    south_west_coordinates = [float(x) for x in south_west_coordinates]
    north_east_coordinates = [float(x) for x in north_east_coordinates]
    latitude_boundaries = [south_west_coordinates[0], north_east_coordinates[0]]
    latitude_range = latitude_boundaries[1] - latitude_boundaries[0]
    longitude_boundaries = [south_west_coordinates[1],
                            north_east_coordinates[1]]
    longitude_range = longitude_boundaries[1] - longitude_boundaries[0]
    for i in range(1, n):
        new_latitude_boundary = latitude_boundaries[0] + i*latitude_range/n
        latitude_boundaries.insert(-1, new_latitude_boundary)
        new_longitude_boundary = longitude_boundaries[0] + i*longitude_range/n
        longitude_boundaries.insert(-1, new_longitude_boundary)
    return latitude_boundaries, longitude_boundaries

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.FileHandler('script_log.txt'))
    logger.setLevel(logging.INFO)

    logger.info(str(datetime.datetime.now()))

    logger.info('Processing tweets...')

    spark_context = pyspark.SparkContext()
    tweets_csv = spark_context.textFile('tweets.csv').map(
        lambda x: x)
    logger.info('first tweet entry: ' + str(tweets_csv.first()))

    logger.info("finished")
