import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print("************ Connection created **************")
    return spark
    
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    print("****************** Reading songs Data ***********************")
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs_table"))
    print("Created songs_table")
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artist_table"))
    print("Created artists_table")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*/*.json"
    print("****************** Reading logs Data ***********************")
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users_table"))
    print("Created users_table")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select("timestamp", hour(df.timestamp).alias('Hour'), dayofmonth(df.datetime).alias('Day_Of_Month'), weekofyear(df.datetime).alias("Week_Of_Year"), month(df.datetime).alias('Month'), year(df.datetime).alias('Year'), dayofweek(df.datetime).alias("Day_Of_Week")).distinct().orderBy("timestamp")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time_table"))
    print("Created time table")
    
    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song-data/A/A/A/*.json"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, (song_df.artist_name == df.artist) &
                              (song_df.title == df.song) & (song_df.duration == df.length)).select(monotonically_increasing_id().alias('songplay_id'), df.timestamp.alias('start_time'), df.userId, df.level, song_df.song_id,song_df.artist_id, df.sessionId, df.location, df.userAgent, year(df.datetime).alias("year"), month(df.datetime).alias("month"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays_table"))
    print("Created songplays_table")


def main():
    print("----------Starting the Spark Data Pipeline---------------")
    print("************ Creating connection to spark *************")
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mysparkdatalakeproject/"
    print("****************** Loading songs table *********************")
    process_song_data(spark, input_data, output_data)   
    print("****************** Loading Logs table *********************")
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
