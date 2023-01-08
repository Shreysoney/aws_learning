from Redshift_Project1.Config.Connection import connect,close_connection
def query_exe(query: str):

    connection=connect()
    cur=connection.cursor()
    cur.execute(query)
    if query.split()[0].lower()=='select':
        return cur.fetchall()
    connection.commit()
    close_connection(connection)




