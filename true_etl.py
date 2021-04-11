import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf, col, year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id, from_unixtime
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, DecimalType as Dec, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as Ts


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function will create spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Call spark session. Build song schema from input data(song_data) for
    correct data types. Create tables for songs and artists. Parquet
    files will go to ouput data location. 

    Arguments:
    spark -- spark session
    input_data -- filepath, where data coming from
    output_data -- filepath, location of parquet files.j

    Return: None
    """
    global songsSchema
    songsSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dec()),
        Fld("artist_longitude", Dec()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int()),
    ]) 
    
    # File path if running on local
    # song_data = f"{input_data}/song_data/*/*/*/*.json"

    # File path if running on aws
    song_data = f"{input_data}song_data/*/*/*/*.json"
    print(f"Reading song data from {song_data}..\n")
    df = spark.read.json(song_data, schema=songsSchema)

    print('Extracting columns for songs table creation..\n')
    df.createOrReplaceTempView("songs_table")
    songs_table = spark.sql('''
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM songs_table
    ''')
    songs_table = songs_table.dropDuplicates(['song_id'])
    
 
    print("Writing the songs table to parquet files partitioned by year and artist..\n")
    songs_table.write.parquet(output_data + "songs_table.parquet", partitionBy=['year', 'artist_id'], mode='overwrite')


    print('Extracting columns for artists table creation..\n')
    artists_table = spark.sql('''
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM songs_table
    ''')
    
    
    print("Writing the artists table to parquet files..\n")
    artists_table.write.parquet(output_data + "artist_table.parquet", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Call spark session. Build song schema from input data (log_data) for
    correct data types. Create tables for songs and artists. Parquet
    files will go to ouput data location. 

    Arguments:
    spark -- spark session
    input_data -- filepath, where data coming from
    output_data -- filepath, location of parquet files.

    Return: None
    """
    
    # File path if running on local
    # log_data = f"{input_data}/log_data/*.json"

    # File path if running on aws
    log_data = f"{input_data}log_data/*/*/*.json"

    print(f"Reading song data from {log_data}..\n")
    df = spark.read.json(log_data)
    

    print("Filtering by actions for song plays..\n")
    df = df.filter(df.page=='NextSong')
    df.createOrReplaceTempView("logs_data_table")

 
    users_table = spark.sql('''
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM logs_data_table
    ''')
    users_table = users_table.dropDuplicates(["userId"])
    
    print("Write users table to parquet files..\n")
    users_table.write.parquet(output_data + 'users_table.parquet', mode='overwrite')

    print("Extract columns to create time table...")
    df = df.withColumn("ts",from_unixtime((df.ts.cast('bigint')/1000)).cast('timestamp'))
    df.createOrReplaceTempView("time_table")
    time_table = df.select(\
                  df.ts.alias('start_time'),    
                  hour(df.ts).alias('hour'), \
                  dayofmonth(df.ts).alias('day'),\
                  weekofyear(df.ts).alias('week'),\
                  month(df.ts).alias('month'),\
                  year(df.ts).alias('year'),\
                  dayofweek(df.ts).alias('weekday'),\
                )
    time_table = time_table.dropDuplicates(["start_time"])
    time_table.write.parquet(output_data + 'time_table.parquet', partitionBy = ['year', 'month'], mode='overwrite')
    
    print("Reading in song data to use for songplays table...\n")
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data, schema=songsSchema)
    song_df.createOrReplaceTempView("songs_table") 
    artists_table = spark.sql('''
        SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM songs_table
    ''')
    artists_table.createOrReplaceTempView("artists_table") 

    print("Extracting columns to create songplays table..\n")
    songplays_table = spark.sql('''
        SELECT 
            year(l.ts) AS year,
            month(l.ts) AS month,
            l.ts AS start_time,
            l.userId AS user_id,
            l.level,
            s.song_id,
            a.artist_id,
            l.sessionId AS session_id,
            l.location,
            l.userAgent AS user_agent
        FROM time_table AS l
        JOIN songs_table AS s 
        ON (l.song = s.title AND l.artist = s.artist_name)  
        JOIN artists_table AS a ON a.artist_id=s.artist_id
        LIMIT 5
    ''')

    print("Creating songplays_id..\n")
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table.write.parquet(output_data + 'songplays.parquet', partitionBy= ['year', 'month'], mode='overwrite')


def main():
    spark = create_spark_session()

    # File location if running on local
    """
    input_data = "./data/"
    output_data = "./sparky_jpa/"
    """
   
    # File location if running on aws
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-jpa/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
