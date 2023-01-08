import psycopg2
from sqlalchemy import create_engine

#Creating Database Connection
try:
    def connect():
        conn_string = "postgresql://postgres:Shrey@localhost:5432/postgres"
        conn1 = psycopg2.connect(conn_string)
        return conn1
    def connectsql():
        conn_string = "postgresql://postgres:Shrey@localhost:5432/postgres"
        db = create_engine(conn_string)
        conn = db.connect()
        return conn

except Exception as e:
    print(e)

def close_connection(conn):
    conn.close()




