import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA= config.get("S3","LOG_DATA")
SONG_DATA= config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging.events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging.songs"
songplay_table_drop = "DROP TABLE IF EXISTS sparkify.songplays"
user_table_drop = "DROP TABLE IF EXISTS sparkify.users"
song_table_drop = "DROP TABLE IF EXISTS sparkify.songs"
artist_table_drop = "DROP TABLE IF EXISTS sparkify.artists"
time_table_drop = "DROP TABLE IF EXISTS sparkify.time"

# CREATE SCHEMA

sparkify_schema_create =  "CREATE SCHEMA IF NOT EXISTS sparkify"
staging_schema_create = "CREATE SCHEMA IF NOT EXISTS staging"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging.events (
artist VARCHAR NULL,
auth VARCHAR NULL,
firstName VARCHAR NULL,
gender VARCHAR NULL,
itemInSession INT,
lastName VARCHAR NULL,
length DECIMAL NULL,
level VARCHAR NULL,
location VARCHAR NULL,
method VARCHAR NULL,
page VARCHAR NULL,
registration DECIMAL,
sessionId INT,
song VARCHAR NULL,
status INT,
ts BIGINT,
userAgent VARCHAR NULL,
userId INT
)""")

staging_songs_table_create = ("""CREATE TABLE staging.songs (
artist_id VARCHAR NULL,
artist_latitude DECIMAL NULL,
artist_location VARCHAR NULL,
artist_longitude DECIMAL NULL,
artist_name VARCHAR NULL,
duration DECIMAL NULL,
num_songs INT,
song_id VARCHAR,
title VARCHAR,
year INT
)""")

songplay_table_create = ("""CREATE TABLE sparkify.songplays (
songplay_id INT IDENTITY(0,1) PRIMARY KEY,
start_time BIGINT NOT NULL REFERENCES sparkify.time(start_time) SORTKEY,
user_id INT REFERENCES sparkify.users(user_id),
level VARCHAR,
song_id VARCHAR NOT NULL REFERENCES sparkify.songs(song_id) DISTKEY,
artist_id VARCHAR NOT NULL REFERENCES sparkify.artists(artist_id),
session_id INT,
location VARCHAR,
user_agent VARCHAR
)""")

user_table_create = ("""CREATE TABLE sparkify.users (
user_id INT PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
) DISTSTYLE ALL;""")

song_table_create = ("""CREATE TABLE sparkify.songs (
song_id VARCHAR PRIMARY KEY DISTKEY,
title VARCHAR,
artist_id VARCHAR NOT NULL REFERENCES sparkify.artists(artist_id),
year INT,
duration DECIMAL
)""")

artist_table_create = ("""CREATE TABLE sparkify.artists (
artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
lattitude DECIMAL,
longitude DECIMAL
) DISTSTYLE ALL;""")

time_table_create = ("""CREATE TABLE sparkify.time (
start_time BIGINT PRIMARY KEY,
hour SMALLINT,
day SMALLINT,
week SMALLINT,
month SMALLINT,
year SMALLINT,
weekday SMALLINT
) DISTSTYLE ALL;""")

# STAGING TABLES

staging_events_copy = ("""COPY staging.events FROM '{}'
    CREDENTIALS 'aws_iam_role={}' 
    FORMAT AS JSON '{}' REGION 'us-west-2';
""").format(LOG_DATA, ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging.songs FROM '{}' 
    CREDENTIALS 'aws_iam_role={}' 
    FORMAT AS JSON 'auto' REGION 'us-west-2';
""").format(SONG_DATA, ROLE_ARN) 

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO sparkify.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent FROM staging.events e 
INNER JOIN staging.songs s ON e.song = s.title AND e.artist = s.artist_name
WHERE e.page='NextSong'
""")

user_table_insert = ("""INSERT INTO sparkify.users (user_id, first_name, last_name, gender, level) 
SELECT DISTINCT userId, firstName, lastName, gender, level 
FROM staging.events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""INSERT INTO sparkify.songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging.songs
WHERE song_Id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO sparkify.artists (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
FROM staging.songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO sparkify.time (start_time, hour, day, week, month, year, weekday)
SELECT ts, 
DATE_PART(hour, tsDate),
DATE_PART(day, tsDate),
DATE_PART(week, tsDate),
DATE_PART(month, tsDate),
DATE_PART(year, tsDate),
DATE_PART(dow, tsDate)
FROM
(SELECT DISTINCT ts, DATEADD(second, ts/1000, '1970-01-01') as tsDate
 FROM staging.events) temp
""")

# QUERY LISTS
create_schema_queries = [staging_schema_create, sparkify_schema_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
