import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
"""
    analyze compression was ran on each table to optimize queries.
"""

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist           VARCHAR(200),
    auth             VARCHAR(20),
    first_mame       VARCHAR(100),
    gender           VARCHAR(1),
    item_in_session  INTEGER,
    last_name        VARCHAR(100),
    length           DECIMAL,
    level            VARCHAR(20),
    location         VARCHAR(100) ENCODE ZSTD,
    method           VARCHAR(10),
    page             VARCHAR(20),
    registration     FLOAT,
    session_id       INTEGER,
    song             VARCHAR(200) ENCODE ZSTD,
    status           INTEGER,
    ts               double precision,
    user_agent       VARCHAR(300) ENCODE ZSTD,
    user_id          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id       VARCHAR(18) NOT NULL,
    num_songs     INTEGER,
    title         VARCHAR(200) NOT NULL ENCODE ZSTD,
    artist_name   VARCHAR(200),
    artist_latitude    NUMERIC(12,5),
    year          INTEGER,
    duration      NUMERIC(12,5),
    artist_id     VARCHAR(20) NOT NULL ENCODE ZSTD,
    artist_longitude   NUMERIC(12,5),
    artist_location    VARCHAR(300)
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id      INTEGER identity(0, 1) PRIMARY KEY DISTKEY,
    start_time       TIMESTAMP NOT NULL SORTKEY REFERENCES time(start_time),
    user_id          INTEGER REFERENCES users(user_id),
    level            VARCHAR(20),
    song_id          VARCHAR(18) NOT NULL REFERENCES songs(song_id),
    artist_id        VARCHAR(20) NOT NULL REFERENCES artists(artist_id),
    session_id       INTEGER,
    location         VARCHAR(100) ENCODE ZSTD,
    user_agent       VARCHAR(300) ENCODE ZSTD
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id     INTEGER PRIMARY KEY DISTKEY SORTKEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    gender      VARCHAR(1),
    level       VARCHAR(20)
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id     VARCHAR(18) PRIMARY KEY DISTKEY SORTKEY,
    title       VARCHAR(200) NOT NULL ENCODE ZSTD,
    artist_id   VARCHAR(20) NOT NULL ENCODE ZSTD,
    year        INTEGER,
    duration    NUMERIC(12,5)
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id   VARCHAR(20) PRIMARY KEY DISTKEY SORTKEY,
    name        VARCHAR(200) ENCODE ZSTD,
    location    VARCHAR(200) ENCODE ZSTD,
    latitude    NUMERIC(12,5),
    longitude   NUMERIC(12,5)
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time  TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
    hour        INTEGER,
    day         INTEGER,
    week        INTEGER,
    month       INTEGER,
    year        INTEGER,
    weekday     INTEGER
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy {} from {}
credentials 'aws_iam_role={}'
json '{}'
region 'us-west-2';
""").format("staging_events", LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy {} from {}
credentials 'aws_iam_role={}'
json 'auto'
region 'us-west-2';
""").format("staging_songs", SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent  
)
SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS ts, 
    se.user_id, 
    se.level, 
    s.song_id, 
    a.artist_id, 
    se.session_id, 
    se.location,
    se.user_agent
FROM staging_events se
LEFT JOIN songs s ON (s.title = se.song)
LEFT JOIN artists a ON (a.name = se.artist)
WHERE s.artist_id = a.artist_id;
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT user_id,
    first_mame,
    last_name,
    gender,
    level
FROM staging_events se 
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs ss
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT ts_convert.ts, 
    date_part(hour, ts_convert.ts)  as hour,
    date_part(day, ts_convert.ts)   as day,
    date_part(week, ts_convert.ts)  as week_of_year,
    date_part(month, ts_convert.ts) as month,
    date_part(year, ts_convert.ts)  as year,
    date_part(dow, ts_convert.ts)   as weekday
FROM(
    SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS ts
    FROM staging_events
) ts_convert
""")

# QUERY LISTS

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert
]
