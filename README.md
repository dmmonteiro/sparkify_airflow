# Sparkify: Data Pipelines with Airflow

This project creates an ETL pipeline to process logs and songs datasets for Sparkify.

Apache Airflow is used to orchestrate the tasks. Data is extracted from an S3 bucket and loaded into a Redshift database in order to optimize data analysis. 

## Datasets

### Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

`song_data/A/B/C/TRABCEI128F424C983.json`
`song_data/A/A/B/TRAABJL12903CDCF1A.json`

Below is an example of what a single song file, `TRAABJL12903CDCF1A.json`, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

`log_data/2018/11/2018-11-12-events.json`
`log_data/2018/11/2018-11-13-events.json`

And below is an example of what the data in a log file, `2018-11-12-events.json`, looks like.

![log dataset preview](/images/log-data.png)

## Database Schema for Song Play Analysis

The star schema was built and optimized for queries on song play analysis. It includes the following tables:

### Staging Tables
 **staging_events**: records in log data files
    
    artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId
 **staging_songs**: records in song data files
    
    num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year

### Fact Table

**songplays**: records in log data associated with song plays i.e. records with page NextSong

    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

**users**: users in the app
        
    user_id, first_name, last_name, gender, level
**songs**: songs in music database
    
    song_id, title, artist_id, year, duration
**artists**: artists in music database
    
    artist_id, name, location, latitude, longitude
**time**: timestamps of records in songplays broken down into specific units
    
    start_time, hour, day, week, month, year, weekday

## Steps to reproduce

1. The solution was built using Python 3.8.5 and Apache Airflow 2.3.2. All dependencies are listed in `requirements.txt`.
To install them, you can run the following command:
    ```
    pip install -r requirements.txt
    ```

2. If you are running Apache Airflow on WSL, you might want to take a look at these instructions:
    - The first time you run Airflow, run: `airflow db init`
    - You may need to create an admin user, which can be done by running:
        `airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin`
        This creates a user `admin` with password `admin`
    - To start the scheduler, run: `airflow scheduler`
    - To open the UI, run: `airflow webserver`
    Airflow should be running in http://localhost:8080

3. Once Airflow is up and running, you will need to create the following connections:
- `aws_credentials`: of type `Amazon Web Services`, representing valid credentials with privileges to manipulate a Redshift database
- `redshift`: of type `Postgres`, pointing to a valid Redshift database
