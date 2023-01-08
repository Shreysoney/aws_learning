import pandas as pd
from Project_Redshift2.Config.Connection import connect,close_connection,connectsql


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
    if 'select' in sql_command.lower():
        data=query_exe(sql_command)
        return data
    else:
        query_exe(sql_command)

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
    if 'select' in query.lower():
        return cur.fetchall()
    connection.commit()
    close_connection(connection)

def csv_to_sql(file:str,table_name:str,file_schema:str):
    ''' Loads csv file data to sql table.
            Parameter:
             file: a string
             table_name: a string
             file_schema: a string
            '''
    df=pd.read_csv(file)
    con_sql=connectsql()
    df.to_sql(table_name, con_sql,schema = file_schema, if_exists='replace',index=False)


def data_to_csv(data:list,columns:list,output_file:str):
    ''' Loads Query Output Data to Csv File.
                Parameter:
                 data: a list
                 columns: list
                 output_file: a string '''
    df=pd.DataFrame(data,columns=columns)
    df.to_csv(output_file,index=False)



