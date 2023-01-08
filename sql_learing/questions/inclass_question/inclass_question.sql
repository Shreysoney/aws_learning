--1. Find out all the cust id where the previous (last only) transaction is more then current transaction ammount?
with all_cust as (
select  sum(tran_ammt) tot_ammount ,tran_date,cust_id from cards_ingest.tran_fact
group by 2,3
),
get_prev as ( select cust_id,tran_date,tot_ammount,
                     nvl(lag(tot_ammount,1) over(partition by cust_id order by tran_date),0) as prev_tot_ammount,
                     coalesce(lag(tran_date,1) over(partition by cust_id order by tran_date),'2022-01-01') as prev_tran_date
              from all_cust )
select * from get_prev where prev_tot_ammount > tot_ammount

--2. Show the cust id, state cd, total trsaction ammount till date (running total) for all the records.
--sum all the trsanction ammount by using window function by cust id, state cd,tran_date
with proc_data as (select cust_id,tran_date,tran_ammt,coalesce(stat_cd,'TX') as stat_cd from cards_ingest.tran_fact),
valid_data as( select cust_id,tran_date,stat_cd,sum(tran_ammt) as tran_amt from proc_data group by cust_id,tran_date,stat_cd)
select *,sum(tran_amt)over(partition by cust_id,stat_cd order by tran_date rows unbounded preceding)as sum_ammt from valid_data


--3. Show all the cust id,state cd, total number of trasaction (Include all the transactio per state cd) and
--total_transaction ammont per state cd, total number of trasaction (don't include any transaction which are less than 1000)
--Note : Here in case 1 you are including all the transaction to show the count but later you have tu just include only where >1000
select cust_id,stat_cd, tran_ammt,count(1)over(partition by stat_cd),sum(case when tran_ammt>1000 then tran_ammt else 0 end)over(partition by stat_cd),sum(case when tran_ammt>1000 then 1 else 0 end)over(partition by stat_cd) as sumtran  from cards_ingest.tran_fact


--4. in cust_dim_details change all the name from Mike to Nike where cust_last_name !='dogeee'

--5.  Show me the month wise transaction ammount and zip_cd
with proc_data as (select * from cards_ingest.tran_fact tt inner join cards_ingest.cust_dim_details ct on tt.cust_id=ct.cust_id)
select sum(tran_ammt),zip_cd, extract(month from tran_date) as month_ext from proc_data group by zip_cd,extract(month from tran_date)  order by extract(month from tran_date)

--6. Show me active customer per month?
with proc_data as (select tt.cust_id,active_flag,tran_date from cards_ingest.tran_fact tt inner join cards_ingest.cust_dim_details ct on tt.cust_id=ct.cust_id)
select cust_id,extract(month from tran_date) as mont from proc_data where active_flag='Y' group by cust_id,extract(month from tran_date) order by mont

--7. delete records which are duplicate from cust_dim_details  table. (Keep the latest record only by date)

--8. Show me all the custid and tranid and total transaction by cust id where you don't have matching record in cust_dim_details
with proc_data as (select tt.cust_id,tran_id,zip_cd,tran_ammt from cards_ingest.tran_fact tt left outer join cards_ingest.cust_dim_details ct on tt.cust_id=ct.cust_id and tran_date between start_date and end_date)
select cust_id,tran_id,sum(tran_ammt)over(partition by cust_id) from proc_data where zip_cd is null


