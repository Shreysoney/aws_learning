from Redshift_Project1.Config.Connection import connectsql,close_connection
from Redshift_Project1.Utils.Query_exec import query_exe
from SQL import query,create_table,count_query,count1_query
import pandas as pd

all_data=query_exe(query)
#Creating Dataframe
df=pd.DataFrame(all_data,columns=['tran_id','cust_id','stat_cd','tran_ammt','tran_date'])

#Filling Null values
df['stat_cd'].fillna('TX',inplace=True)

#Adding New Column Commision
df['commison'] =df['tran_ammt']*4
print(df)

#Checking All States are not null
for i in df['stat_cd'].isnull():
    if i==True:
        print("Some State is Null")
        break
else:
    print("All State_cd data is good")
#query_exe(create_table)
#Inserting Data from Dataframe to Postgre Table
conn=connectsql()
df.to_sql('tran_fact2', conn,schema = 'cards_ingest', if_exists='replace',index=False)
print(pd.read_sql('cards_ingest.tran_fact2',conn))
close_connection(conn)
count1=query_exe(count_query)
count2=query_exe(count1_query)
if count1==count2:
    print("Same Number of Records Loaded")
else:
    print("Not same Number of Records Loaded")
