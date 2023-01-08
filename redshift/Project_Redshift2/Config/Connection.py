import psycopg2
from sqlalchemy import *
from sqlalchemy.engine.url import URL

#Creating Database Connection
try:
    def connect():

        #engine = create_engine(url_object)
        conn1 = psycopg2.connect(host='redshift-cluster-1.c7nyeu5pvlfp.us-east-1.redshift.amazonaws.com',
            database='dev',
            user='awsuser1',
            password='xyz',port=5439)
        return conn1

    def connectsql():

        url_object = URL.create(
            'postgresql',
            host='redshift-cluster-1.c7nyeu5pvlfp.us-east-1.redshift.amazonaws.com',
            database='dev',
            username='awsuser1',
            password='xyz',port=5439
        )

        conn = create_engine(url_object)
        return conn

except Exception as e:
    print(e)

def close_connection(conn):
    conn.close()




