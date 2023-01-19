
--1. Total unique customer per day.

select tran_date,count(distinct(cust_id)) from cards_ingest.tran_fact3 group by tran_date

--2. Total number of unique customer till date

with proc_data as(
    select tran_id,cust_id,tran_date,row_number()over(partition by cust_id order by tran_date) as counter 
	from cards_ingest.tran_fact3 order by tran_id),
	
valid_data as(
 select cust_id,tran_date,counter,(case when counter=1 then 1 else 0 end) as f_count 
 from proc_data order by tran_date),
 
fn_data as
 (select cust_id,tran_date,sum(f_count)over(order by tran_date rows unbounded preceding )as count_unique from valid_data)
 
select tran_date,max(count_unique)as unique_cust from fn_data group by tran_date

--3. Total transaction amount per customer per day ( if its C then add if D then subtract )

with valid_data as 
(select *,case when tran_type='D' then -tran_ammount else tran_ammount end as tran_ammt 
 from cards_ingest.tran_fact3)
 
select cust_id,sum(tran_ammt) from valid_data group by cust_id


--4. Find out duplicate transaction in total.

with proc_data as
(select * ,count(*) as cnt from  cards_ingest.tran_fact3 group by tran_id,cust_id,tran_date,tran_ammount,tran_type)

select tran_id,cust_id,tran_date,tran_ammount,tran_type from proc_data where cnt>1 

--5. show the transaction which has debit but never credit before.

with proc_data as 
(select tran_id,cust_id,tran_date,tran_type,case when tran_type='C' then 1 else 0 end as card_info from cards_ingest.tran_fact3),

valid_data as 
(select *, sum(card_info)over(partition by cust_id order by tran_id,tran_date rows unbounded preceding ) as sum_info from proc_data)

select * from valid_data where tran_type='D' and sum_info=0



