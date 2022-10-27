import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("DWH", "DWH_IAM_ROLE_ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")                         

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays cascade"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist           text,
auth             text,
firstName        text,
gender           text,
iteminSession    int,
lastName         text,
length           float,
level            text,
location         text,
method           text,
page             text,
registration     text,
sessionId        int,
song             text,
status           int,
ts               bigint,
userAgent        text,
userId           int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
num_songs        int,
artist_id        text,
artist_latitude  text,
artist_longitude text, 
artist_location  text, 
artist_name      text,
song_id          text,
title            text,
duration         float,
year             int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id   int IDENTITY (0,1) NOT NULL sortkey distkey, 
start_time    bigint NOT NULL, 
user_id       text NOT NULL, 
level         text, 
song_id       text, 
artist_id     text, 
session_id    text NOT NULL, 
location      text, 
user_agent    text);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id      text, 
first_name   text NOT NULL, 
last_name    text NOT NULL, 
gender       text, 
level        text) diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
song_id      text NOT NULL, 
title        text NOT NULL, 
artist_id    text, 
year         int, 
duration     float NOT NULL) diststyle all;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
artist_id    text NOT NULL sortkey, 
name         text NOT NULL, 
location     text, 
latitude     text, 
longitude    text) diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
start_time   bigint NOT NULL sortkey, 
hour         int, 
day          int, 
week         int, 
month        int, 
year         int, 
weekday      text
) diststyle all;
""")

# STAGING TABLES

staging_songs_copy = ("""
COPY staging_songs
FROM {} iam_role '{}' 
FORMAT AS JSON 'auto'
""").format(SONG_DATA, DWH_ROLE_ARN)

staging_events_copy = ("""
COPY staging_events
FROM {} iam_role '{}' 
FORMAT AS JSON 's3://udacity-dend/log_json_path.json' 
""").format(LOG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT se.ts, 
       CASE
                WHEN se.auth not LIKE 'Logged Out' 
                THEN CAST(se.userId AS text)
                ELSE CAST('N/A' AS text)
                end::text AS user_id,
       se.level, 
       ss.song_id,
       ss.artist_id,
       se.sessionId, 
       se.location, 
       CASE
                WHEN se.auth not LIKE 'Logged Out' 
                THEN CAST(se.userAgent AS text)
                ELSE CAST('N/A' AS text)
                end::text AS user_agent
FROM   staging_events se left join staging_songs ss
       on se.song=ss.title and se.artist=ss.artist_name
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
          CASE
                WHEN se.auth not LIKE 'Logged Out' 
                THEN CAST(se.userId AS text)
                ELSE CAST('N/A' AS text)
                end::text AS user_id,
          CASE
                WHEN se.auth not LIKE 'Logged Out' 
                THEN CAST(se.firstName AS text)
                ELSE CAST('N/A' AS text)
                end::text AS first_name,
          CASE
                WHEN se.auth not LIKE 'Logged Out' 
                THEN CAST(se.lastName AS text)
                ELSE CAST('N/A' AS text)
                end::text AS last_name,
          CASE
                WHEN se.auth not LIKE 'Logged Out' 
                THEN CAST(se.gender AS text)
                ELSE CAST('N/A' AS text)
                end::text AS gender,
       se.level AS level
FROM
         (
           SELECT userId, MAX(ts) AS mts
           FROM staging_events
		   GROUP BY userId
         ) t1 JOIN staging_events AS se ON t1.userId = se.userId AND se.ts = t1.mts
;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, 
           CASE
                WHEN ss.artist_latitude is not NULL
                THEN CAST(ss.artist_latitude AS text)
                ELSE CAST('N/A' AS text)
                end::text AS latitude,
           CASE
                WHEN ss.artist_longitude is not NULL 
                THEN CAST(ss.artist_longitude AS text)
                ELSE CAST('N/A' AS text)
                end::text AS longitude
FROM staging_songs AS ss;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
select distinct
       se.ts as start_time,
       extract('hour' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as hour,
       extract('day' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as day,
       extract('week' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as week,
       extract('month' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as month,
       extract('year' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as year,
       extract('weekday' from (timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second')) as weekday
from staging_events se
WHERE se.ts is not NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
