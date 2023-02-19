from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col,dense_rank,desc
from pyspark.sql.window import Window
import argparse
from pyspark.sql.functions import count

# Session Creation 
spark = SparkSession.builder \
      .master("local[1]").enableHiveSupport() \
      .appName("SparkLearing") \
      .getOrCreate()


# Create command-line argument parser
parser = argparse.ArgumentParser()
# Add optional arguments
parser.add_argument('--Firstdate', '-f')
parser.add_argument('--Seconddate', '-s')
# Parse arguments from terminal
args = parser.parse_args()

#Reading Data
#df=spark.sql("""select * from src_customer.customer_pyspark where data_date between "2022-01-01" and "2022-01-07" """)
df=spark.sql(f"""select * from src_customer.customer_pyspark where data_date between '{args.Firstdate}' and '{args.Seconddate}'""")

#Total number of account_id by data_date as count_accont_by_date
df1=df.groupBy(col("data_date")).agg(count("account_id")).withColumnRenamed("count(account_id)","count_account_by_date")
#Total number of account_id by account_type as count_account_by_type
df2=df.groupBy(col("account_type")).agg(count("account_id")).withColumnRenamed("count(account_id)","count_account_by_type")

#Creating field account_stats by ranking
df3=df2.withColumn("account_stats1",dense_rank().over(Window.orderBy(col("count_account_by_type").desc())))

# Adding values[ 'First','Second','Third'] by ranking
df4=df3.withColumn("account_stats",when(col("account_stats1")==1,"First").when(col("account_stats1")==2,"Second").otherwise("Third"))
df4.select("account_type","count_account_by_type","account_stats").show()


#join df with df1 on account_open_dt to get the count_accont_by_date
dfjoin=df.join(df1,df.data_date==df1.data_date,"inner").select("account_id","account_open_dt","acct_hldr_primary_addr_state","acct_hldr_primary_addr_zip_cd","acct_hldr_first_name","acct_hldr_last_name","dataset_date","load_time","Name","account_type",df.data_date,"count_account_by_date")
#Join dfjoin with df2  on account_type to get count_accont_by_type
df2join=dfjoin.join(df2,dfjoin.account_type==df2.account_type,"inner").select("account_id","account_open_dt","acct_hldr_primary_addr_state","acct_hldr_primary_addr_zip_cd","acct_hldr_first_name","acct_hldr_last_name","dataset_date","load_time","Name",df2.account_type,"data_date","count_account_by_date","count_account_by_type")
# df2join with df4 on account_type to get account_stats
df3join=df2join.join(df4,df2join.account_type==df4.account_type,"inner").select("account_id","account_open_dt","acct_hldr_primary_addr_state","acct_hldr_primary_addr_zip_cd","acct_hldr_first_name","acct_hldr_last_name","dataset_date","load_time","Name",df4.account_type,"data_date","count_account_by_date",df4.count_account_by_type,"account_stats")

df3join.write.partitionBy("dataset_date").parquet("s3://sparkdatapro/Sparkquestionfinal/")


print("------All Done---------------")