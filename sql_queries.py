import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    ARTIST          VARCHAR,
    AUTH            VARCHAR,
    first_name      VARCHAR,
    GENDER          VARCHAR,
    ITEMINSESSION   VARCHAR,
    LAST_NAME        VARCHAR,
    LENGTH          NUMERIC(10,2),
    LEVEL           VARCHAR,
    LOCATION        VARCHAR,
    METHOD          VARCHAR,
    PAGE            VARCHAR,
    REGISTRATION    VARCHAR,
    sessionid       VARCHAR,
    SONG            VARCHAR,
    STATUS          INT,
    TS              bigint,
    USERAGENT       VARCHAR,
    USER_ID         INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    NUM_SONGS          INT,
    ARTIST_ID          VARCHAR,
    ARTIST_LATITUDE    NUMERIC(10,2),
    ARTIST_LONGITUDE   NUMERIC(10,2),
    ARTIST_LOCATION    VARCHAR,
    ARTIST_NAME        VARCHAR,
    SONG_ID            VARCHAR,
    TITLE              VARCHAR,
    DURATION           NUMERIC(10,2),
    YEAR               INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    SONGPLAY_ID   INT PRIMARY KEY,
    START_TIME    TIMESTAMP NOT NULL,
    USER_ID       INT NOT NULL,
    LEVEL         VARCHAR,
    SONG_ID       VARCHAR,
    ARTIST_ID     VARCHAR,
    SESSION_ID    INT,
    LOCATION      VARCHAR,
    USER_AGENT    VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS USERS (
    USER_ID      VARCHAR PRIMARY KEY,
    FIRST_NAME   VARCHAR,
    LAST_NAME    VARCHAR,
    GENDER       VARCHAR,
    LEVEL        VARCHAR
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGS (
    SONG_ID     VARCHAR  PRIMARY KEY,
    title       VARCHAR,
    ARTIST_ID   VARCHAR,
    YEAR        INT,
    DURATION    NUMERIC (10, 2)
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    ARTIST_ID   VARCHAR PRIMARY KEY,
    NAME        VARCHAR,
    LOCATION    VARCHAR,
    LATTITUDE   DOUBLE PRECISION,
    LONGITUDE   DOUBLE PRECISION
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS TIME (
    START_TIME   TIMESTAMP PRIMARY KEY,
    HOUR         INT,
    DAY          VARCHAR,
    WEEK         VARCHAR,
    MONTH        VARCHAR,
    YEAR         INT,
    WEEKDAY      VARCHAR
    );
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} \
                      iam_role {} \
                      region 'us-west-2' FORMAT AS JSON {} ;\
                      """).format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data'
                        iam_role ''
                        region 'us-west-2' format as json 'auto';
                      """) 

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays( songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values(%s,%s,%s,%s,%s,%s,%s,%s,%s);
""")

user_table_insert = ("""
insert into users( user_id, first_name, last_name, gender, level)
values(%s,%s,%s,%s,%s);
""")

song_table_insert = ("""
insert into songs( song_id, title, artist_id, year, duration)
values(%s,%s,%s,%s,%s);
""")

artist_table_insert = ("""
insert into artists( artist_id, name, location, lattitude, longitude)
values(%s,%s,%s,%s,%s);
""")

time_table_insert = ("""
insert into time( start_time, hour, day, week, month, year, weekday)
values(%s,%s,%s,%s,%s,%s,%s); 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
