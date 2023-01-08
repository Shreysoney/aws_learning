import psycopg2
import Project_Redshift3.Config.Connection as db
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


# Creating Database Connection
try:
    def connect():

        conn1=psycopg2.connect(host=db.host,
                         database=db.database,
                         user=db.user,
                         password=db.password)

        return conn1

    def connectsql():

        url_object = URL.create(
            'postgresql',
            host=db.host,
            database=db.database,
            username=db.user,
            password=db.password,port=5432
        )
        conn = create_engine(url_object)
        return conn

except Exception as e:
    print(e)


def close_connection(conn):
    conn.close()

def create_table():
    conn=connect()
    cur=conn.cursor()
    query='''Create table if not exists cards_ingest.big_data (
    order_id int,
    brand_name varchar(20),
    product_name varchar(20),
   sales_ammount decimal(9,2),
   sales_date date ) '''
    cur.execute(query)
    conn.commit()
