import configparser
import psycopg2
from sql_queries import select_count_rows_queries


def query_results(cur, conn):
    """
    Get the total count of rows stored into each table
    """
    for query in select_count_rows_queries:
        print("Results for " + query)
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            print('Count: ', row[0])




def main():
    """
    Execute queries on the staging and dimensional tables to check that rows are inserted successfully
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    query_results(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()