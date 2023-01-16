from Inclass_question6_proj.Utils.Query_exec import data_to_csv,query_exe,read_sql_file
from inclass_question6 import generate_dummy_info
from s3 import create_buc,check_buc_name,upload,create_buc_obj

#Creating Product Data
data = generate_dummy_info()
data_to_csv(data=data,columns=['product_name', 'cost_ammount'],output_file='product_data_20230401.csv')

#Creating Product Table
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Inclass_question6_proj/SQL/create_table_product_data.sql')

#Loading Product Data to S3 using AWS Cli
#aws s3 cp product_data_20230401.csv s3://redshifts3demo or
upload(file_name='order_data_20230401.csv',bucket_name='redshifts3demo')

#Loading to Redshift from S3
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Inclass_question6_proj/SQL/Loading_order_data_to_redshift.sql')


#Creating View
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Inclass_question6_proj/SQL/create_view_procost_data.sql')
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Inclass_question6_proj/SQL/create_view_profit.sql')
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Inclass_question6_proj/SQL/create_view_total_profit_company.sql')


#Displaying View
proc_data=query_exe('''select * from cards_ingest1.vw_procost_data''')
data_to_csv(data=proc_data,columns=['order_id','brand_name','product_name',
            'cost_ammt','sale_ammt','sales_date'],print_csv=False,print_df=True)

proc_data=query_exe('''select * from cards_ingest1.vw_profit ''')
data_to_csv(data=proc_data,columns=['order_id','brand_name','product_name',
            'cost_ammt','sale_ammt','sales_date','profit','profit_per','profit_group'],print_csv=False,print_df=True)

proc_data=query_exe('''select * from cards_ingest1.vw_total_profit_per_company_month''')
data_to_csv(data=proc_data,columns=['brand_name','sales_year',
            'sales_month','total_profit','total_products','Rank'],print_csv=False,print_df=True)



#Creating New Bucket
create_buc(bucket_name='tranformdata')
#Check Whether Bucket exists or not and shows all buckets list
check_buc_name(bucket_name='tranformdata')

#Adding folder names
create_buc_obj(bucket_name='tranformdata',folder_name='vw_procost/')
create_buc_obj(bucket_name='tranformdata',folder_name='vw_profit_grp/')
create_buc_obj(bucket_name='tranformdata',folder_name='vw_profit_company_rnk/')



#Unloading Data to S3
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Inclass_question6_proj/SQL/unload_vw_procost_data.sql')
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Inclass_question6_proj/SQL/unload_vw_profit.sql')
read_sql_file(sql_file='C:/Users/Shrey/PycharmProjects/pythonProject/Inclass_question6_proj/SQL/unload_vw_profit_per_company.sql')





