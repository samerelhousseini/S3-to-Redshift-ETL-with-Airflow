class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """) 

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)


    check_key_in_songs= ("""
        select count(*) from songs 
        where songid is null
    """)

    check_key_in_artists= ("""
        select count(*) from artists 
        where artistid is null
    """)

    check_key_users = ("""
        select count(*) from users 
        where userid is null
    """)

    check_key_time = ("""
        select count(*) from dimTime 
        where start_time is null
    """)

    check_key_in_songplays= ("""
        select count(*) from songplays 
        where playid is null
    """)

    tests = [check_key_in_songs, check_key_in_artists, check_key_users, check_key_time, check_key_in_songplays]
    results = [0, 0, 0, 0, 0]