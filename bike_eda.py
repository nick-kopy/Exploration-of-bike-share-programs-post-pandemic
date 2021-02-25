# This file contains all the necessary functions for exploration.ipynb to run
# It mostly collects, cleans, and presents data

import pyspark as ps
from pyspark.sql.types import *
import pyspark.sql.functions as f
import pandas as pd
import matplotlib.pyplot as plt

def la_csv_to_sdf(spark_session):
  '''
  Returns a spark dataframe of LA's bikeshare data with schema.

  Because timestamp data comes in two formats it must be either
  pulled in and assessed by spark separately, or converted to Pandas
  later which can correctly interpret a column with multiple
  formats. LA's data is small enough that it's easier to do it with
  Pandas outside of this function.
  '''

  la_schema = StructType([
    StructField('trip_id', IntegerType(), True), # primary key
    StructField('duration', IntegerType(), True), # duration in minutes
    StructField('start_time', StringType(), True), # TimestampType drops 2020 data because it is in mm/dd/yyyy
    StructField('end_time', StringType(), True),
    StructField('start_station', IntegerType(), True), # foreign key, station names exist online somewhere
    StructField('start_lat', FloatType(), True), # useful if looking for geographic data
    StructField('start_lon', FloatType(), True),
    StructField('end_station', IntegerType(), True),
    StructField('end_lat', FloatType(), True),
    StructField('end_lon', FloatType(), True),
    StructField('bike_id', IntegerType(), True), # foreign key
    StructField('plan_duration', IntegerType(), True), # days rider has had their pass, 1 for "walk up"
    StructField('trip_route_category', StringType(), True), # "Round Trip" or "One Way"
    StructField('passholder_type', StringType(), True)
  ])

  return spark_session.read.csv('data/LA/', header='true', inferSchema=False, schema=la_schema)

def la_month_graph(spark_session, ax, hline=True):
  '''
  Makes a graph of monthly bike rides for 2018-2020
  '''

  # First grabs the data
  la_sdf = la_csv_to_sdf(spark_session)

  # Filters out columns we don't need
  use_cols = ['duration', 'start_time']
  drop_cols = la_sdf.columns
  for col in use_cols:
    drop_cols.remove(col)

  # Moves data to pandas
  la_month_df = la_sdf.drop(*drop_cols).toPandas()

  # Uses start_time column as both a timestamp and a ride count
  la_month_df['start_time'] = pd.to_datetime(la_month_df['start_time'], infer_datetime_format=True)

  # Take each year, group by month, and count up rides
  la_month18 = la_month_df[la_month_df['start_time'].dt.year == 2018]
  la_month18 = la_month18.groupby(la_month18['start_time'].dt.month).size()

  la_month19 = la_month_df[la_month_df['start_time'].dt.year == 2019]
  la_month19 = la_month19.groupby(la_month19['start_time'].dt.month).size()

  la_month20 = la_month_df[la_month_df['start_time'].dt.year == 2020]
  la_month20 = la_month20.groupby(la_month20['start_time'].dt.month).size()

  # Start graphing
  x = [1,2,3,4,5,6,7,8,9,10,11,12]

  ax.plot(x, la_month18, label='2018', linewidth=3)
  ax.plot(x, la_month19, label='2019', linewidth=3)
  ax.plot(x, la_month20, label='2020', linewidth=3)

  # two horizontal lines that signify 2020 events
  if hline == True:
    ax.axvline(x= 1 + 26/31)
    ax.text(2, 31000, 'First LA\nCOVID case', fontsize=12)
    ax.axvline(x= 3 + 17/31)
    ax.text(3.7, 30000, 'Shelter in\nplace order', fontsize=12)

  # Graph peripharies make it more meaningful
  ax.set_xticks(x)
  ax.set_xticklabels(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])

  ax.set_xlabel('month', fontsize=18)
  ax.set_ylabel('rides taken', fontsize=18)
  ax.set_title("LA's bikeshare rides per month", fontsize=22)

  ax.legend()

def dummy_code_users_ch(col):
    if col == 'Subscriber' or col == 'member':
        return 1
    else:
        return 0

def ch_csv_to_sdf(spark_session):
  '''
  Returns a spark dataframe of Chicago's bikeshare data with schema.

  Data comes in three different formats so it's processed in three sets.
  Different from LA, we decide columns to keep in this step since different
  sets actually have different data.
  '''
  ch_sdf_set1 = spark_session.read.csv(['data/CH/Divvy_Trips_2018_Q1.csv', 'data/CH/Divvy_Trips_2019_Q2.csv'], header='true')

  # If you want more columns you have to do this for all three sets.
  use_cols = ['01 - Rental Details Local End Time', '01 - Rental Details Local Start Time', 'User Type']
  drop_cols = ch_sdf_set1.columns
  for col in use_cols:
      drop_cols.remove(col)

  # Drop columns we don't want and rename what we're keeping
  ch_sdf_set1 = (ch_sdf_set1.drop(*drop_cols)
                .withColumnRenamed('01 - Rental Details Local Start Time', 'start_time')
                .withColumnRenamed('01 - Rental Details Local End Time', 'end_time')
                .withColumnRenamed('user type', 'usertype'))

  # Tweak one column so it's usable
  udf = f.UserDefinedFunction(dummy_code_users_ch, IntegerType())
  ch_sdf_set1 = ch_sdf_set1.withColumn('usertype', udf('usertype'))

  # Now we start set 2 of 3
  filelist = []
  for quarter in [2,3,4]:
      filelist.append('data/CH/Divvy_Trips_2018_Q{}.csv'.format(quarter))
  for quarter in [1,3,4]:
      filelist.append('data/CH/Divvy_Trips_2019_Q{}.csv'.format(quarter))

  # Make the dataframe
  ch_sdf_set2 = spark_session.read.csv(filelist, header='true')

  # Pick which columns we want
  use_cols = ['start_time', 'end_time', 'usertype']
  drop_cols = ch_sdf_set2.columns
  for col in use_cols:
      drop_cols.remove(col)

  # Drop the columns we don't want (no need to rename here)
  ch_sdf_set2 = ch_sdf_set2.drop(*drop_cols)

  # Tweaking the one column, like above
  ch_sdf_set2 = ch_sdf_set2.withColumn('usertype', udf('usertype'))

  # And finally set 3 of 3
  filelist = ['data/CH/Divvy_Trips_2020_Q1.csv']
  for month in range(4,13):
      filelist.append('data/CH/2020{0:0=2d}-divvy-tripdata.csv'.format(month))

  # Make the dataframe
  ch_sdf_set3 = spark_session.read.csv(filelist, header='true')

  # Pick which columns we want
  use_cols = ['started_at', 'ended_at', 'member_casual']
  drop_cols = ch_sdf_set3.columns
  for col in use_cols:
      drop_cols.remove(col)

  # Drop the columns we don't want and rename what we're keeping
  ch_sdf_set3 = (ch_sdf_set3.drop(*drop_cols)
                .withColumnRenamed('started_at', 'start_time')
                .withColumnRenamed('ended_at', 'end_time')
                .withColumnRenamed('member_casual', 'usertype'))

  # Tweak our usertype column one last time
  ch_sdf_set3 = ch_sdf_set3.withColumn('usertype', udf('usertype'))

  # return the complete spark DF
  return ch_sdf_set1.union(ch_sdf_set2).union(ch_sdf_set3)

def ch_month_graph(spark_session, ax, hline=True):
  '''
  Makes a graph of monthly bike rides from 2018-2020
  '''
  # First grabs the data
  ch_sdf = ch_csv_to_sdf(spark_session)

  # Make a few columns to organize around
  ch_sdf_months = (ch_sdf.withColumn('date', ch_sdf.start_time.cast(DateType()))
              .withColumn('year', f.date_format('date', 'y'))
              .withColumn('month', f.date_format('date', 'M/L')))

  # Get trip counts per month per year
  ch_sdf_months18 = ch_sdf_months.filter(ch_sdf_months.year == 2018).groupBy('month').count().withColumnRenamed('count', '2018_ct')
  ch_sdf_months19 = ch_sdf_months.filter(ch_sdf_months.year == 2019).groupBy('month').count().withColumnRenamed('count', '2019_ct')
  ch_sdf_months20 = ch_sdf_months.filter(ch_sdf_months.year == 2020).groupBy('month').count().withColumnRenamed('count', '2020_ct')

  # Puts the counts together, takes a minute because data is big
  ch_sdf_months = ch_sdf_months18.join(ch_sdf_months19, ['month']).join(ch_sdf_months20, ['month'])

  # Move to pandas for visualization
  ch_df_months = ch_sdf_months.toPandas()

  # Sort months
  ch_df_months.sort_values(by=['month'], inplace=True)

  # Plot years
  ax.plot(ch_df_months['month'], ch_df_months['2018_ct'], label='2018', linewidth=3)
  ax.plot(ch_df_months['month'], ch_df_months['2019_ct'], label='2019', linewidth=3)
  ax.plot(ch_df_months['month'], ch_df_months['2020_ct'], label='2020', linewidth=3)

  # two horizontal lines that signify 2020 events
  if hline == True:
    ax.axvline(x= 1 + 24/31)
    ax.text(1.9, 300000, 'First Chicago\nCOVID case', fontsize=12)
    ax.axvline(x= 3 + 26/31)
    ax.text(3.9, 450000, 'Stay at\nhome order', fontsize=12)

  # Graph peripharies to make it more meaningful
  ax.set_xticks(ch_df_months['month'])
  ax.set_xticklabels(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])

  ax.set_xlabel('month', fontsize=18)
  ax.set_ylabel('rides taken', fontsize=18)
  ax.set_title("Chicago's bikeshare rides per month", fontsize=22)

  ax.legend()