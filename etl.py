import configparser
import psycopg2
import pandas as pd
from sql_queries import *
from sql_queries import copy_table_queries, insert_table_queries,song_select


def load_staging_tables(cur, conn):
    """ Description: This function can be used to populate the staging tables.

       Arguments:
       cur: the cursor object. 
       conn: database object. 

       Returns: None """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
       Description: This function can be used to populate the fact and dimension tables.

       Arguments:
       cur: the cursor object. 
       conn: database object. 

       Returns:None
       """
    users_df = pd.read_sql(""" select distinct user_id, first_name, last_name, gender, level from staging_events """,conn)
    for i, row in users_df.iterrows():
        cur.execute(insert_table_queries[1], list(row))
        conn.commit()
        
    artists_df = pd.read_sql ("""select distinct artist_id, ARTIST_NAME, ARTIST_LOCATION, ARTIST_LATITUDE, ARTIST_LONGITUDE from    staging_songs""",conn)
    for i,row in artists_df.iterrows():
        cur.execute(insert_table_queries[3], list(row))
        conn.commit()
    
    songs_df = pd.read_sql(""" select distinct song_id, title, artist_id, year, duration from staging_songs""",conn)
    for i , row in songs_df.iterrows():
        cur.execute(insert_table_queries[2], list(row))
        conn.commit()
    
    time_data = pd.read_sql(""" select distinct ts from staging_events where page = 'NextSong'""",conn)
    t = time_data['ts'].apply(pd.to_datetime)
    time_data['ts'] =pd.to_datetime(time_data['ts'])
    time_data = { 'Time':time_data['ts'],'hour':time_data['ts'].dt.hour, 'day':time_data['ts'].dt.day_name(), 
                     'week of year':time_data['ts'].dt.weekofyear,'month':time_data['ts'].dt.month,
                     'year':time_data['ts'].dt.year,'weekday':time_data['ts'].dt.weekday}
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(insert_table_queries[4], list(row))
        conn.commit()
    
    song_play = pd.read_sql("""select distinct ts,user_Id ,level,song ,artist,sessionId,location ,userAgent from staging_events""",conn)
    df = pd.DataFrame(song_play)
    df['ts'] =pd.to_datetime(df['ts'])
    for index, row in df.iterrows():
        # insert songplay record
        songplay_data = (index,row.ts,row.userId ,row.level,row.song ,row.artist,row.sessionId,row.location ,row.userAgent)
        cur.execute(insert_table_queries[0], songplay_data)

def main():
    """
       Description: This function can be used can be used to read 
                    the database config file to connect the redshift 
                    databse and completes the etl process.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print(conn)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()