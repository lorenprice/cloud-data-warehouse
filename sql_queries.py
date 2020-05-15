import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# IAM_ROLE
ARN = config.get('IAM_ROLE', 'ARN')

# S3
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
SONG_SUBSET = config.get('S3', 'SONG_SUBSET')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender char(1),
    itemInSession int,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    sessionId int,
    song varchar,
    status int,
    ts numeric,
    userAgent varchar,
    userId int);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric,
    year int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender text, 
    level text);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration numeric);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar, 
    location varchar, 
    latitude numeric, 
    longitude numeric)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {}
    iam_role {}
    REGION 'us-west-2'
    FORMAT AS JSON {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs from {}
    iam_role {}
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
""").format(SONG_DATA, ARN) # SONG_DATA if using full data, SONG_SUBSET if testing

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
    start_time, user_id, level, song_id,
    artist_id, session_id, location, user_agent)
    SELECT
        TIMESTAMP 'epoch' + se.ts /1000 *INTERVAL '1 second' as start_time,
        se.userId as user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId as session_id,
        se.location,
        se.userAgent as user_agent
    FROM staging_events se
    LEFT JOIN staging_songs ss ON se.artist=ss.artist_name AND se.song=ss.title
    WHERE se.page='NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (
    user_id, first_name, last_name, 
    gender, level)
    SELECT
        se.userId,
        se.firstName,
        se.lastName,
        se.gender,
        se.level
    FROM staging_events se
    JOIN (  SELECT 
                MAX(ts) as ts,
                userId
            FROM staging_events
            WHERE page='NextSong'
            GROUP BY userId) mt
    ON se.userId=mt.userId AND se.ts=mt.ts;
""")

song_table_insert = ("""
    INSERT INTO songs (
    song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (
    artist_id, name, location, latitude, longitude)
    SELECT 
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (
    start_time, hour, day, week, month, year, weekday)
    SELECT
        start_time,
        EXTRACT(hour FROM start_time)    AS hour,
        EXTRACT(day FROM start_time)     AS day,
        EXTRACT(week FROM start_time)    AS week,
        EXTRACT(month FROM start_time)   AS month,
        EXTRACT(year FROM start_time)    AS year,
        EXTRACT(weekday FROM start_time) AS weekday
    FROM (
        SELECT 
        DISTINCT TIMESTAMP 'epoch' + se.ts /1000 *INTERVAL '1 second' as start_time
        FROM staging_events se
        WHERE page='NextSong')
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
