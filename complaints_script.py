import datetime
import logging
import os
import sys

import pyspark
import pyspark.sql as sql
import pyspark.sql.functions as functions
import pyspark.sql.types as types

import grid

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
LOGGER.addHandler(logging.FileHandler('script_log.txt'))
sc = pyspark.SparkContext()

# From https://stackoverflow.com/a/36218558 .
def sparkImport(module_name, module_directory):
    """
    Convenience function. 
    
    Tells the SparkContext sc (must already exist) to load
    module module_name on every computational node before
    executing an RDD. 
    
    Args:
        module_name: the name of the module, without ".py". 
        module_directory: the path, absolute or relative, to
                          the directory containing module
                          module_Name. 
    """
    module_path = os.path.abspath(
        module_directory + "/" + module_name + ".py")
    sc.addPyFile(module_path)

# Add all scripts from repository to local path. 
# From https://stackoverflow.com/a/35273613 .
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

ss = sql.SparkSession(sc)

complaints_df_schema = types.StructType([
    types.StructField('CMPLNT_NUM', types.IntegerType(),
                      nullable=False),
    types.StructField('CMPLNT_FR_DT', types.StringType()),
    types.StructField('CMPLNT_FR_TM', types.StringType()),
    types.StructField('CMPLNT_TO_DT', types.StringType()),
    types.StructField('CMPLNT_TO_TM', types.StringType()),
    types.StructField('RPT_DT', types.StringType(), nullable=False),
    types.StructField('KY_CD', types.StringType()),
    types.StructField('OFNS_DESC', types.StringType()),
    types.StructField('PD_CD', types.IntegerType()),
    types.StructField('PD_DESC', types.StringType()),
    types.StructField('CRM_ATPT_CPTD_CD', types.StringType()),
    types.StructField('LAW_CAT_CD', types.StringType()),
    types.StructField('JURIS_DESC', types.StringType()),
    types.StructField('BORO_NM', types.StringType()),
    types.StructField('ADDR_PCT_CD', types.StringType()),
    types.StructField('LOC_OF_OCCUR_DESC', types.StringType()),
    types.StructField('PREM_TYP_DESC', types.StringType()),
    types.StructField('PARKS_NM', types.StringType()),
    types.StructField('HADEVELOPT', types.StringType()),
    types.StructField('X_COORD_CD', types.FloatType()),
    types.StructField('Y_COORD_CD', types.FloatType()),
    types.StructField('Latitude', types.FloatType()),
    types.StructField('Longitude', types.FloatType()),
    types.StructField('Lat_Lon', types.StringType())])

complaints_df = ss.read.csv(
    "crime_complaints_with_header.csv",
    header=True,
    schema=complaints_df_schema)

complaints_df = complaints_df \
    .drop('CMPLNT_NUM') \
    .drop('CMPLNT_FR_TM') \
    .drop('CMPLNT_TO_TM') \
    .drop('RPT_DT') \
    .drop('KY_CD') \
    .drop('OFNS_DESC') \
    .drop('PD_CD') \
    .drop('PD_DESC') \
    .drop('CRM_ATPT_CPTD_CD') \
    .drop('LAW_CAT_CD') \
    .drop('JURIS_DESC') \
    .drop('BORO_NM') \
    .drop('ADDR_PCT_CD') \
    .drop('LOC_OF_OCCUR_DESC') \
    .drop('PREM_TYP_DESC') \
    .drop('PARKS_NM') \
    .drop('HADEVELOPT') \
    .drop('X_COORD_CD') \
    .drop('Y_COORD_CD') \
    .drop('Lat_Lon')

# Filter to find the complaints which have an exact date of occurrence
# or which have a start and end date.

complaints_df = complaints_df \
    .filter(~complaints_df.CMPLNT_FR_DT.isNull())

def string_to_date(s):
    if s == None:
        return None
    else:
        return datetime.datetime.strptime(s, '%m/%d/%Y')

string_to_date_udf = functions.udf(string_to_date, types.DateType())

# Now get the actual column dates.

complaints_df = complaints_df \
    .withColumn(
        'FR_DT',
        string_to_date_udf(complaints_df.CMPLNT_FR_DT)) \
    .drop('CMPLNT_FR_DT') \
    .withColumn('TO_DT',
                string_to_date_udf(complaints_df.CMPLNT_TO_DT)) \
    .drop('CMPLNT_TO_DT')

# Now filter for complaints which occur on one day only. 

complaints_df = complaints_df \
    .filter(complaints_df.TO_DT.isNull() | (complaints_df.TO_DT == complaints_df.FR_DT)) \
    .drop(complaints_df.TO_DT) \
    .withColumnRenamed('FR_DT', 'Date')

# Filter for complaints occurring within the past month.

date_to_column = functions.lit(datetime.datetime(2015, 3, 3))
date_from_column = functions.lit(functions.date_sub(date_to_column, 31))
complaints_df = complaints_df.filter(
    (complaints_df.Date < date_to_column) & (complaints_df.Date >= date_from_column))

# Compute grid square for each crime. 

# Southwest corner of New York:
# lat = 40.488320, lon = -74.290739
# Northeast corner of New York:
# lat = 40.957189, lon = -73.635679

latlongrid = grid.LatLonGrid(
    lat_min=40.488320,
    lat_max=40.957189,
    lon_min=-74.290739,
    lon_max=-73.635679,
    lat_step=grid.get_lon_delta(1000, (40.957189 - 40.488320)/2.0),
    lon_step=grid.get_lat_delta(1000))

def grid_square_from_lat_lon(lat, lon):
    return latlongrid.grid_square_index(lat=lat, lon=lon)
grid_square_from_lat_lon_udf = functions.udf(grid_square_from_lat_lon, returnType=types.IntegerType())

grid_square_column = grid_square_from_lat_lon_udf(complaints_df.Latitude, complaints_df.Longitude)

complaints_df = complaints_df \
    .withColumn('GridSquare', grid_square_column) \
    .drop('Latitude') \
    .drop('Longitude')

# Now count by (GridSquare, Date).

complaints_df = complaints_df \
    .groupBy(complaints_df.GridSquare, complaints_df.Date) \
    .count()

LOGGER.debug(complaints_df.take(10))
LOGGER.debug(complaints_df.count())
