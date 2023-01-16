import pandas as pd
import Inclass_question6_proj.Config.Connection as db
import psycopg2
from sqlalchemy import *
from sqlalchemy.engine.url import URL


#Creating Database Connection
try:
    def connect():

        conn1=psycopg2.connect(host=db.host,
                         database=db.database,
                         user=db.user,
                         password=db.password,port=db.port)

        return conn1

    def connectsql():

        url_object = URL.create(
            'postgresql',
            host=db.host,
            database=db.database,
            username=db.user,
            password=db.password,port=db.port
        )
        conn = create_engine(url_object)
        return conn

except Exception as e:
    print(e)


def close_connection(conn):
    conn.close()

def query_exe(query: str):
    ''' Connects with the database and executes the query and checks whether it is a select query or some other query.
        Parameter:
         query: a string
        Return:
         Query_Output if it is a select query
        '''
    connection=connect()
    cur=connection.cursor()
    cur.execute(query)
    if 'select' in query.lower() and 'create' not in query.lower() and 'unload' not in query.lower():
        return cur.fetchall()
    connection.commit()
    close_connection(connection)

def data_to_csv(data:list,columns:list,print_csv=True,print_df=False,**kwargs):
    ''' Loads Query Output Data to Csv File.
                Parameter:
                 data: a list
                 columns: list
                 output_file: a string '''
    df=pd.DataFrame(data,columns=columns)
    if print_df:
        print(df)
    if print_csv:
        df.to_csv(kwargs['output_file'],index=False,header=False)



def read_sql_file(sql_file:str):
    '''Opens and reads the file as a single buffer and
     passes the generated command to query_exe function.
    Parameter:
     sql_file: It is string
    Return:
     Query_Output if it is a select query
    '''
    read_sql= open(sql_file, 'r')
    sql_command = read_sql.read()
    read_sql.close()
    if 'select' in sql_command.lower() and 'create' not in sql_command.lower() and 'unload' not in sql_command.lower():
        data=query_exe(sql_command)
        return data
    else:
        query_exe(sql_command)