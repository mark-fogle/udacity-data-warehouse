import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, drop_table_queries, create_table_queries, create_schema_queries


def load_staging_tables(cur, conn):
    """
    Loads data from JSON files in S3 to staging tables in Redshift
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables into fact and dimension tables
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    """
    Drops table if they exist
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates tables for staging and sparkify schemas
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def create_schema(cur, conn):
    """
    Creates staging and sparkify schemas
    """
    for query in create_schema_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    create_schema(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()