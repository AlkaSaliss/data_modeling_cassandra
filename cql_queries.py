# Define all queries that will be used in the notebook

create_keyspace_query = """
    CREATE KEYSPACE IF NOT EXISTS big_sparkify
    WITH REPLICATION = 
        {'class': 'SimpleStrategy', 'replication_factor': 1}
"""

# drop tables queries
drop_table1 = "DROP TABLE IF EXISTS music_play_sessions"
drop_table2 = "DROP TABLE IF EXISTS users_activities"
drop_table3 = "DROP TABLE IF EXISTS songs_records"
list_drop_queries = [drop_table1, drop_table2, drop_table3]


## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during 
## sessionId = 338, and itemInSession = 4
create_music_play_sessions = """
    CREATE TABLE music_play_sessions (
        session_id int,
        item_in_session int,
        artist text,
        song text,
        length float,
        user_id int,
        first_name text,
        last_name text,
        gender text,
        level text,
        location text,
        PRIMARY KEY (session_id, item_in_session)
    )
"""

insert_music_play_sessions = """
    INSERT INTO music_play_sessions (
        session_id ,item_in_session, artist, song, length, user_id ,first_name, last_name, gender, level, location
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

select_query1 = """
    SELECT artist, song, length from music_play_sessions WHERE session_id = 338 AND item_in_session = 4
"""


## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

create_users_activities = """
    CREATE TABLE users_activities (
        user_id int,
        session_id int,
        item_in_session int,
        artist text,
        song text,
        length float,
        first_name text,
        last_name text,
        gender text,
        level text,
        location text,
        PRIMARY KEY (user_id, session_id, item_in_session)
    )
"""

insert_users_activities = """
    INSERT INTO users_activities (
        user_id, session_id, item_in_session, artist, song, length, first_name, last_name, gender, level, location
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

select_query2 = """
    SELECT artist, song, first_name, last_name from users_activities WHERE user_id = 10 AND session_id = 182
"""


## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
create_songs_records = """
    CREATE TABLE songs_records (
        song text,
        session_id int,
        item_in_session int,
        artist text,
        length float,
        user_id int,
        first_name text,
        last_name text,
        gender text,
        level text,
        location text,
        PRIMARY KEY (song, session_id, item_in_session)
    )
"""

insert_songs_records = """
    INSERT INTO songs_records (
        song, session_id, item_in_session, artist, length, user_id, first_name, last_name, gender, level, location
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

select_query3 = """
    SELECT first_name, last_name from songs_records WHERE song = 'All Hands Against His Own'
"""
