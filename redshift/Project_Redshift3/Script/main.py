from Project_Redshift3.Utils.Query_exec import connectsql,create_table
import generate_data as gd
import pandas as pd


create_table()
record_num_request = 5000
df = pd.DataFrame(gd.generate_dummy_info(record_num_request),
                  columns=['orderid', 'brand_name', 'product_name', 'sales_ammount', 'sales_date'])
conn=connectsql()
df.to_sql('big_data',conn,schema='cards_ingest' ,if_exists='replace',index=False)