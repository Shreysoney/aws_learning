from Project_Redshift2.Utils.Query_exec import read_sql_file,csv_to_sql,data_to_csv

#Creating Schema
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/SQL/create_schema_ingest.sql')
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/SQL/create_schema_lkp_data.sql')

#Creating Table
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/SQL/cust_dim_details_table.sql')
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/SQL/tran_fact_table.sql')
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/SQL/lkp_state_details_table.sql')

#Loading Data
csv_to_sql(file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/Data/cust_dim.csv',table_name='cust_dim_details',file_schema='cards_ingest')
csv_to_sql(file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/Data/lkp_data.csv',table_name='lkp_state_details',file_schema='lkp_data')
csv_to_sql(file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/Data/tran_fact.csv',table_name='tran_fact',file_schema='cards_ingest')

#Creating Csv Files from Sql Commands
data=read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/SQL/Question3.sql')
data_to_csv(data=data,output_file='Question3.csv',columns=['cust_id','stat_cd','tran_date','unique_running_state_cust','unique_state_customers'])
data=read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/SQL/Question4.sql')
data_to_csv(data=data,output_file='Question4.csv',columns=['tran_date','statee_cd','count','value','rank_data'])
data=read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Project_Redshift2/SQL/Question5.sql')
data_to_csv(data=data,output_file='Question5.csv',columns=['potential_customer_cnt','population_cnt','statee_cd','count_per_date'])
