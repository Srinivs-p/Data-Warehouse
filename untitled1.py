import configparser
import psycopg2
import pandas as pd
from sql_queries import *
from sql_queries import copy_table_queries, insert_table_queries,song_select




def insert_tables(cur, conn):  
    song_play = pd.read_sql("""select distinct ts,user_Id ,length,level,song ,artist,sessionId,location ,userAgent from staging_events s left  join (
   SELECT s.song_id, a.artist_id
   FROM songs s
   JOIN artists a
   ON s.artist_id = a.artist_id)a
    on s.artist = a.artist_id ;""",conn)
    df = pd.DataFrame(song_play)
    df['ts'] =pd.to_datetime(df['ts'])
    for i, row in df.iterrows():
        # insert songplay record
        songplay_data = (i,row.ts,row.user_id ,row.level,row.song ,row.artist,row.sessionid,row.location ,row.useragent)
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
    
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()