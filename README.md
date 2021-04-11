# The Sparkify ETL Data Lake

## Project Overview
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake.. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, we will building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

<br>

## Project Files
- dl.cfg
- etl.py
- test.ipynb


**dwh.cfg** - This file will be used to store our credentials on Amazon. For security,
the exact credentials must be store on your computer and must not be shared in public.

**etl.py** - The script makes a Spark session, reads data from S3 bucket and writes it back to S3 bucket.

**test.ipynb** - To check the schema and database.
<br>


## ETL pipeline

There are two main functions on our ETL Pipeline, the ```process_song_data``` and ```process_log_data```

### Process Song Data
This function processes song data by iterating over the JSON files coming from the input folder and creates ```songs``` and ```artists``` table. These will write to S3 bucket defined in the ```output_data```.

### Process Log Data
Like ```process_song_data```,  this processes log data by iterating over the JSON files coming from the input folder and creates ```users```, ```times```,  and  ```songplays``` table. These will write to S3 bucket defined in the ```output_data```.

## Running The Scripts

In order to run the scripts in AWS, make sure you have an account,  created an AWS EMR Cluster and have Jupyter Notebook inside it. Copy the scripts from ```etl.py``` and run it from the Jupyter Notebook. You can run also the 
```test.ipynb``` to check the database.




