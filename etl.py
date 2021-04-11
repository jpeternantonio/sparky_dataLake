import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, DecimalType as Dec, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as Ts
from pyspark.sql.functions import monotonically_increasing_id, from_unixtime

# config = configparser.ConfigParser()

# config.read_file(open('dl.cfg'))

# os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
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
    
    
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*/*.json"
    
    # read song data file
    print(f"Reading song data from {song_data}..\n")
    df = spark.read.json(song_data, schema=songsSchema)

    # extract columns to create songs table
    print('Extracting columns for songs table creation..\n')
    df.createOrReplaceTempView("songs_table")
    songs_table = spark.sql('''
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM songs_table
    ''')
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing the songs table to parquet files partitioned by year and artist..\n")
    songs_table.write.parquet(output_data + "songs_table.parquet", partitionBy=['year', 'artist_id'], mode='overwrite')

    # extract columns to create artists table
    print('Extracting columns for artists table creation..\n')
    artists_table = spark.sql('''
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM songs_table
    ''')
    
    
    # write artists table to parquet files
    print("Writing the artists table to parquet files..\n")
    artists_table.write.parquet(output_data + "artist_table.parquet", mode='overwrite')


def process_log_data(spark, input_data, output_data):

    # get filepath to log data file
    log_data = f"{input_data}/log_data/*.json"

    # read log data file
    print(f"Reading song data from {log_data}..\n")
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    print("Filtering by actions for song plays..\n")
    df = df.filter(df.page=='NextSong')
    df.createOrReplaceTempView("logs_data_table")

    # extract columns for users table    
    users_table = spark.sql('''
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM logs_data_table
    ''')
    users_table = users_table.dropDuplicates(["userId"])
    
    # write users table to parquet files
    print("Write users table to parquet files..\n")
    users_table.write.parquet(output_data + 'users_table.parquet', mode='overwrite')

      # create datetime column from original timestamp column 
    # print("Create datetime column from original timestamp column...")
    # get_datetime = udf(lambda time: datetime.fromtimestamp((time/1000.0)), Date())
    # df = df.withColumn("date",get_datetime("ts")) 
    # print("done.")
    
    
    # create timestamp column from original timestamp column 
    # print("Create timestamp column from original timestamp column...")
    # convert_ts = udf(lambda time: datetime.fromtimestamp((time/1000.0)), Ts())
    # df = df.withColumn("ts",convert_ts("ts")) 
    # print("done.")
    
    # # extract columns to create time table
    # print("Extracting columns to create time table..\n")
    # df.createOrReplaceTempView("time_table")
    # time_table = spark.sql('''
    #     SELECT ts AS start_time, 
    #         date_format(date,'YYYY') AS year,
    #         date_format(date,'MM') AS month,
    #         date_format(date,'dd') AS day,
    #         date_format(date,'w') AS week,
    #         date_format(ts,'E') AS weekday,
    #         HOUR(ts) AS hour
    #     FROM time_table
    # ''')
    # time_table = time_table.dropDuplicates(['start_time'])
    
    # # write time table to parquet files partitioned by year and month
    # print("Writing time table to parquet files partitioned by year and month..\n")
    # time_table.write.parquet(output_data + 'time_table.parquet', partitionBy = ['year', 'month'], mode='overwrite')
    df = df.withColumn("ts",from_unixtime((df.ts.cast('bigint')/1000)).cast('timestamp'))
    
    # extract columns to create time table
   
    
    # write time table to parquet files partitioned by year and month
    print("Extract columns to create time table...")
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

    # create columns year and month (partition)
    df = df.withColumn("year",year(df.ts).alias('year'))
    df = df.withColumn("month",month(df.ts).alias('month'))
    # read in song data to use for songplays table
    print("Reading in song data to use for songplays table...\n")
    # song_df = spark.read.parquet(f'{output_data}songs_table.parquet')
    # song_df.createOrReplaceTempView("songs_table")
    # artist_df = spark.read.parquet(f'{output_data}artist_table.parquet')
    # artist_df.createOrReplaceTempView("artists_table")

    # sqlContext = SQLContext(spark)
    # song_df = sqlContext.read.parquet(f'{output_data}/songs_table.parquet') 
    # song_df.createOrReplaceTempView("songs_table")
    # artist_df = sqlContext.read.parquet(f'{output_data}/artist_table.parquet')
    # artist_df.createOrReplaceTempView("artists_table")
    # extract columns from joined song and log datasets to create songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data, schema=songsSchema)
    song_df.createOrReplaceTempView("songs_table") 

    print("Extract columns from joined song and log datasets to create songplays table...")
    artists_table = spark.sql('''
        SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM songs_table
    ''')
    artists_table.createOrReplaceTempView("artists_table") 
    print("done.")

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

    # write songplays table to parquet files partitioned by year and month
    print("Creating songplays_id..\n")
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table.write.parquet(output_data + 'songplays.parquet', partitionBy= ['year', 'month'], mode='overwrite')


def main():
    spark = create_spark_session()
    input_data = "./data/"
    output_data = "./sparky_jpa/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
