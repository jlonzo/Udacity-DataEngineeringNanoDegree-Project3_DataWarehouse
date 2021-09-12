import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS songs;"
artist_table_drop         = "DROP TABLE IF EXISTS artists;"
time_table_drop           = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist         VARCHAR
        ,auth          VARCHAR
        ,firstName     VARCHAR
        ,gender        VARCHAR
        ,itemInSession SMALLINT
        ,lastName      VARCHAR
        ,length        NUMERIC
        ,level         VARCHAR
        ,location      VARCHAR
        ,method        VARCHAR
        ,page          VARCHAR sortkey
        ,registration  NUMERIC
        ,sessionId     INT
        ,song          VARCHAR
        ,status        INT
        ,ts            BIGINT
        ,userAgent     VARCHAR
        ,userId        VARCHAR    
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs          SMALLINT
        ,artist_id         VARCHAR
        ,artist_latitude   NUMERIC
        ,artist_longitude  NUMERIC
        ,artist_location   VARCHAR
        ,artist_name       VARCHAR
        ,song_id           VARCHAR
        ,title             VARCHAR
        ,duration          NUMERIC
        ,year              SMALLINT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) [PRIMARY KEY]
        ,start_time TIMESTAMP NOT NULL
        ,user_id    VARCHAR NOT NULL
        ,level      VARCHAR
        ,song_id    VARCHAR NOT NULL
        ,artist_id  VARCHAR NOT NULL
        ,session_id INT
        ,location   VARCHAR
        ,user_agent VARCHAR
    ) sortkey (start_time);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     VARCHAR NOT NULL [PRIMARY KEY]
        ,first_name VARCHAR 
        ,last_name  VARCHAR
        ,gender     CHAR(1)
        ,level      VARCHAR
    ) compound sortkey(first_name, last_name);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR NOT NULL [PRIMARY KEY]
        ,title      VARCHAR NOT NULL
        ,artist_id  VARCHAR NOT NULL
        ,year       SMALLINT
        ,duration   NUMERIC(10,6)
    ) distkey(artist_id)
    sortkey(title);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id  VARCHAR NOT NULL [PRIMARY KEY]
        ,name      VARCHAR NOT NULL
        ,location  VARCHAR
        ,latitude  NUMERIC
        ,longitude NUMERIC
    ) sortkey(name);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL [PRIMARY KEY]
        ,hour    SMALLINT
        ,day     SMALLINT
        ,week    SMALLINT
        ,month   SMALLINT
        ,year    SMALLINT
        ,weekday SMALLINT
    ) distkey(year)
    sortkey(start_time);
""")


# STAGING TABLES
staging_events_copy = ("""
    copy staging_events 
    from {}
    credentials {}
    region 'us-west-2'
    json {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs
    from {}
    credentials {}
    json 'auto' 
    region 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))


# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts / 1000000) * INTERVAL '1 second'
           ,e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
    FROM staging_events e 
    JOIN staging_songs s ON e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration  
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id,first_name,last_name,gender,level)
    SELECT DISTINCT userId, firstName, lastName, gender, level 
    FROM staging_events;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,title,artist_id,year,duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration 
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,name,location,latitude,longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts / 1000000) * INTERVAL '1 second'
           ,date_part(h, (TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts / 1000000) * INTERVAL '1 second'))
           ,date_part(d, (TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts / 1000000) * INTERVAL '1 second'))
           ,date_part(w, (TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts / 1000000) * INTERVAL '1 second'))
           ,date_part(mon, (TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts / 1000000) * INTERVAL '1 second'))
           ,date_part(y, (TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts / 1000000) * INTERVAL '1 second'))
           ,date_part(weekday, (TIMESTAMP WITHOUT TIME ZONE 'epoch' + (ts / 1000000) * INTERVAL '1 second'))
    FROM staging_events;    
""")


# QUERY LISTS
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]