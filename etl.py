import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    '''
        Description: This function can be used to load the staging tables from S3.
        
        Arguments:
            cur: cursor
            conn: connection to the database
            
        Returns:
            None
    '''
    print('Loading staging tables...')
    for query in copy_table_queries:
        
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
        Description: This function can be used to insert the data into the production tables of the warehouse from the staging tables.
        
        Arguments:
            cur: cursor
            conn: connection to the database
            
        Returns:
            None
    '''
    print('Inserting data into DWH tables...')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()