# This file contains all the necessary functions for exploration.ipynb to run
# It mostly collects, cleans, and presents data

import pyspark as ps
from pyspark.sql.types import *
import pyspark.sql.functions as f
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess

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
  Makes a graph of monthly bike rides for 2018-2020.

  Recommended axis input:
  fig, ax = plt.subplots(figsize=(12,8))
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
  # Scale down by 1000 for graph readability
  la_month18 = la_month_df[la_month_df['start_time'].dt.year == 2018]
  la_month18 = la_month18.groupby(la_month18['start_time'].dt.month).size()/1000

  la_month19 = la_month_df[la_month_df['start_time'].dt.year == 2019]
  la_month19 = la_month19.groupby(la_month19['start_time'].dt.month).size()/1000

  la_month20 = la_month_df[la_month_df['start_time'].dt.year == 2020]
  la_month20 = la_month20.groupby(la_month20['start_time'].dt.month).size()/1000

  # Start graphing
  x = [1,2,3,4,5,6,7,8,9,10,11,12]

  ax.plot(x, la_month18, label='2018', linewidth=3)
  ax.plot(x, la_month19, label='2019', linewidth=3)
  ax.plot(x, la_month20, label='2020', linewidth=3)

  # two horizontal lines that signify 2020 events
  if hline == True:
    ax.axvline(x= 1 + 26/31)
    ax.text(2, 31, 'First LA\nCOVID case', fontsize=12)
    ax.axvline(x= 3 + 17/31)
    ax.text(3.7, 30, 'Shelter in\nplace order', fontsize=12)

  # Graph peripharies make it more meaningful
  ax.set_xticks(x)
  ax.set_xticklabels(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])

  ax.set_xlabel('month', fontsize=18)
  ax.set_ylabel('rides taken (thousands)', fontsize=18)
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
  Makes a graph of monthly bike rides from 2018-2020.

  Recommended axis input:
  fig, ax = plt.subplots(figsize=(12,8))
  '''
  # First grabs the data
  ch_sdf = ch_csv_to_sdf(spark_session)

  # Make a few columns to organize around
  ch_sdf_months = (ch_sdf.withColumn('date', ch_sdf.start_time.cast(DateType()))
              .withColumn('year', f.date_format('date', 'y'))
              .withColumn('month', f.date_format('date', 'M')))

  # Get trip counts per month per year
  ch_sdf_months18 = ch_sdf_months.filter(ch_sdf_months.year == 2018).groupBy('month').count().withColumnRenamed('count', '2018_ct')
  ch_sdf_months19 = ch_sdf_months.filter(ch_sdf_months.year == 2019).groupBy('month').count().withColumnRenamed('count', '2019_ct')
  ch_sdf_months20 = ch_sdf_months.filter(ch_sdf_months.year == 2020).groupBy('month').count().withColumnRenamed('count', '2020_ct')

  # Puts the counts together, takes a minute because data is big
  ch_sdf_months = ch_sdf_months18.join(ch_sdf_months19, ['month']).join(ch_sdf_months20, ['month'])

  # Move to pandas for visualization
  ch_df_months = ch_sdf_months.toPandas()

  # Sort by month
  ch_df_months = ch_df_months.astype('int32').sort_values(by=['month'])

  # Scale the values down for readability
  ch_df_months['2018_ct'] = ch_df_months['2018_ct']/1000
  ch_df_months['2019_ct'] = ch_df_months['2019_ct']/1000
  ch_df_months['2020_ct'] = ch_df_months['2020_ct']/1000

  # Plot years
  ax.plot(ch_df_months['month'], ch_df_months['2018_ct'], label='2018', linewidth=3)
  ax.plot(ch_df_months['month'], ch_df_months['2019_ct'], label='2019', linewidth=3)
  ax.plot(ch_df_months['month'], ch_df_months['2020_ct'], label='2020', linewidth=3)

  # two horizontal lines that signify 2020 events
  if hline == True:
    ax.axvline(x= 1 + 24/31)
    ax.text(1.9, 300, 'First Chicago\nCOVID case', fontsize=12)
    ax.axvline(x= 3 + 26/31)
    ax.text(3.9, 450, 'Stay at\nhome order', fontsize=12)

  # Graph peripharies to make it more meaningful
  ax.set_xticks(ch_df_months['month'])
  ax.set_xticklabels(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])

  ax.set_xlabel('month', fontsize=18)
  ax.set_ylabel('rides taken (thousands)', fontsize=18)
  ax.set_title("Chicago's bikeshare rides per month", fontsize=22)

  ax.legend()

def la_diff_scores(spark_session):
  '''
  Returns a pd dataframe of ride count growth (/shrinkage) from 2019 to 2020
  '''

  # First grabs the data
  la_sdf = la_csv_to_sdf(spark_session)

  # Grab only the singular column we need
  use_cols = ['start_time']
  drop_cols = la_sdf.columns
  for col in use_cols:
      drop_cols.remove(col)

  la_sdf_dy = la_sdf.drop(*drop_cols)

  # Port to pandas and use datetime
  la_df_dy = la_sdf_dy.toPandas()
  la_df_dy['start_time'] = pd.to_datetime(la_df_dy['start_time'], infer_datetime_format=True)

  # Collect ride counts for every single day
  la_df_dy = (la_df_dy.groupby(la_df_dy['start_time'].dt.date).count()
                  .rename(columns={'start_time':'ride_ct'})
                  .reset_index()
                  .rename(columns={'start_time':'date'}))

  # Re-instate date as datetime
  la_df_dy['date'] = pd.to_datetime(la_df_dy['date'], infer_datetime_format=True)

  return la_df_dy

def ch_diff_scores(spark_session):
  '''
  Returns a pd dataframe of ride count growth (/shrinkage) from 2019 to 2020
  '''

  # First grabs the data
  ch_sdf = ch_csv_to_sdf(spark_session)

  # Filter spark dataframe for only what we need
  ch_sdf_dy = (ch_sdf.withColumn('date', ch_sdf.start_time.cast(DateType()))
              .drop(*['start_time', 'end_time', 'usertype'])
              .groupBy('date').count()
              .withColumnRenamed('count', 'ride_ct'))

  # Port to pandas, change data type, and sort
  ch_df_dy = ch_sdf_dy.toPandas()

  ch_df_dy['date'] = pd.to_datetime(ch_df_dy['date'], infer_datetime_format=True)

  ch_df_dy.sort_values(by='date', ignore_index=True, inplace=True)

  return ch_df_dy

def la_diff_graph(spark_session, fig, ax1, ax2):
  '''
  Graphs difference scores showing 2019 growth and 2020 growth.

  Scatterplot is daily ride count changes from one year to another,
  while blue line is a LOWESS trend line.

  Recommended axis input:
  fig, (ax1, ax2) = plt.subplots(2, figsize=(18,8))
  '''

  # Grab our daily ride counts
  la_df_dy = la_diff_scores(spark_session)

  # Remove feb 29th, 2020
  la_df_dy = la_df_dy[~((la_df_dy['date'].dt.month == 2) & (la_df_dy['date'].dt.day == 29))]

  # Calculate difference scores
  la_1920_diff = la_df_dy[la_df_dy['date'].dt.year == 2020].reset_index()['ride_ct'] - la_df_dy[la_df_dy['date'].dt.year == 2019].reset_index()['ride_ct']
  la_1819_diff = la_df_dy[la_df_dy['date'].dt.year == 2019].reset_index()['ride_ct'] - la_df_dy[la_df_dy['date'].dt.year == 2018].reset_index()['ride_ct']

  # Make an x axis and trend lines
  x = [x for x in range(1, 366)]
  low19 = lowess(la_1819_diff, x)[:,1]
  low20 = lowess(la_1920_diff, x)[:,1]

  # '18 '19 plot
  ax1.scatter(x, la_1819_diff)
  ax1.plot(low19, linewidth=3, color='b')

  ax1.set_xticks([m for m in range(1,360,30)])
  ax1.set_xticklabels(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])
  ax1.set_ylim(-1000, 1000)

  ax1.set_ylabel('Difference score\n(ride count)', fontsize=18)
  ax1.set_title("2019 growth", fontsize=18)

  # '19 '20 plot
  ax2.scatter(x, la_1920_diff)
  ax2.plot(low20, linewidth=3, color='b')

  ax2.set_xticks([m for m in range(1,360,30)])
  ax2.set_xticklabels(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])
  ax2.set_ylim(-1000, 1000)

  ax2.set_xlabel('month', fontsize=18)
  ax2.set_ylabel('Difference score\n(ride count)', fontsize=18)
  ax2.set_title("2020 growth", fontsize=18)

  fig.suptitle("Daily Difference Scores for LA's Bike Share", fontsize=22)

def ch_diff_graph(spark_session, fig, ax1, ax2):
  '''
  Graphs difference scores showing 2019 growth and 2020 growth.

  Scatterplot is daily ride count changes from one year to another,
  while blue line is a LOWESS trend line.

  Recommended axis input:
  fig, (ax1, ax2) = plt.subplots(2, figsize=(18,8))
  '''

  # Grab our daily ride counts
  ch_df_dy = ch_diff_scores(spark_session)

  # Remove feb 29th, 2020
  ch_df_dy = ch_df_dy[~((ch_df_dy['date'].dt.month == 2) & (ch_df_dy['date'].dt.day == 29))]

  # Calculate difference scores
  ch_1920_diff = ch_df_dy[ch_df_dy['date'].dt.year == 2020].reset_index()['ride_ct'] - ch_df_dy[ch_df_dy['date'].dt.year == 2019].reset_index()['ride_ct']
  ch_1819_diff = ch_df_dy[ch_df_dy['date'].dt.year == 2019].reset_index()['ride_ct'] - ch_df_dy[ch_df_dy['date'].dt.year == 2018].reset_index()['ride_ct']

  # Make an x axis and trend lines
  x = [x for x in range(1, 366)]
  low19 = lowess(ch_1819_diff, x)[:,1]
  low20 = lowess(ch_1920_diff, x)[:,1]

  # '18 '19 plot
  ax1.scatter(x, ch_1819_diff)
  ax1.plot(low19, linewidth=3, color='b')

  ax1.set_xticks([m for m in range(1,360,30)])
  ax1.set_xticklabels(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])
  ax1.set_ylim(-15000, 15000)

  ax1.set_ylabel('Difference score\n(ride count)', fontsize=18)
  ax1.set_title("2019 growth", fontsize=18)

  # '19 '20 plot
  ax2.scatter(x, ch_1920_diff)
  ax2.plot(low20, linewidth=3, color='b')

  ax2.set_xticks([m for m in range(1,360,30)])
  ax2.set_xticklabels(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'])
  ax2.set_ylim(-15000, 15000)

  ax2.set_xlabel('month', fontsize=18)
  ax2.set_ylabel('Difference score\n(ride count)', fontsize=18)
  ax2.set_title("2020 growth", fontsize=18)

  fig.suptitle("Daily Difference Scores for Chicago's Bike Share", fontsize=22)
