--1. Join  cards_ingest.tran_fact with lkp_state_details on state cd. Make sure if any Null Values from fact remove those records
--Show me tran_date,state, number of customer per tran_date and state and number of customer company can target for promotion who are not customer in but still lives in the state (population - number of customer)
with join_data as (select * from cards_ingest.tran_fact ct INNER JOIN lkp_data.lkp_state_details lk on ct.stat_cd=lk.state_cd ),
 all_data as (SELECT population_cnt,tran_date,state_cd,count(DISTINCT(cust_id)) as count_per_date  from join_data GROUP BY tran_date,state_cd,population_cnt
ORDER BY tran_date,state_cd)
SELECT *,(population_cnt-count_per_date) as counterr from all_data order by tran_date,state_cd



--2. To reach each remaining potential_customer_cnt it cost 5$, then show me the states where company has to spend 2nd high $ amount in each date.
--(make sure do potential_customer_cnt -allready customer count to get remaining potential customer count)
with join_data as (select * from cards_ingest.tran_fact ct INNER JOIN lkp_data.lkp_state_details lk on ct.stat_cd=lk.state_cd ),
valid_data as ( SELECT potential_customer_cnt,tran_date,state_cd,count(DISTINCT(cust_id)) as count_per_date  from join_data GROUP BY tran_date,state_cd,potential_customer_cnt 
ORDER BY tran_date,state_cd),
proce_data as(SELECT tran_date,state_cd ,(potential_customer_cnt-count_per_date) as countt,(potential_customer_cnt-count_per_date)*5 as valuee from valid_data ORDER BY tran_date,state_cd),
all_data as (SELECT tran_date,state_cd,countt,valuee,rank()over(PARTITION BY tran_date ORDER BY valuee DESC) as rank_data from proce_data order by tran_date,valuee)
SELECT * from all_data where rank_data=2 


--3. Same as question 1. But the number of customer from transaction table is total number of unique customer till that date .
--(Hint use window function)
with join_data as (select * from cards_ingest.tran_fact ct INNER JOIN lkp_data.lkp_state_details lk on ct.stat_cd=lk.state_cd ),
proc_data as(SELECT cust_id,stat_cd,tran_date,population_cnt,row_number()over(partition by cust_id,stat_cd order by tran_date) as rnk from join_data order by stat_cd,tran_date),
valid_data as(select *,(case when rnk=1 then rnk else 0 end) as cnt from proc_data),
all_data as (select cust_id,stat_cd,tran_date,sum(cnt)over(partition by stat_cd rows unbounded preceding ) as unique_running_state_cust from valid_data),
final_data as(select * , coalesce(lag(unique_running_state_cust,1)over(partition by stat_cd order by tran_date),0)as prev from all_data)
select cust_id,stat_cd, tran_date,unique_running_state_cust,(case when unique_running_state_cust=prev then 0 else unique_running_state_cust end) as unique_state_customers from final_data


--4. Same as question 2. If state cd is NULL  and cust_id is cust_109 then make sure to change to TX  else CA and calculate states where
--company has to spend 2nd lowest $ amount from .
with proc_data as (select tran_id,cust_id,coalesce(stat_cd,case when cust_id='cust_109' then 'TX' else 'CA' end) as statee_cd ,tran_ammt,tran_date from cards_ingest.tran_fact),
join_data as (select * from proc_data ct INNER JOIN lkp_data.lkp_state_details lk on ct.statee_cd=lk.state_cd),
count_data as(SELECT potential_customer_cnt,tran_date,statee_cd,count(DISTINCT(cust_id)) as count_per_date  from join_data GROUP BY tran_date,statee_cd,potential_customer_cnt 
ORDER BY tran_date,statee_cd),
proce_data as(SELECT tran_date,statee_cd ,(potential_customer_cnt-count_per_date) as countt,(potential_customer_cnt-count_per_date)*5 as valuee from count_data ORDER BY tran_date,statee_cd),
all_data as (SELECT tran_date,statee_cd,countt,valuee,rank()over(PARTITION BY tran_date ORDER BY valuee DESC) as rank_data from proce_data order by tran_date,valuee)
SELECT * from all_data where rank_data=2 


--5. Show me the total number of customer company has , total population and potential_customer_cnt across all the states
with proc_data as (select tran_id,cust_id,coalesce(stat_cd,case when cust_id='cust_109' then 'TX' else 'CA' end) as statee_cd ,tran_ammt,tran_date from cards_ingest.tran_fact),
join_data as (select * from proc_data ct INNER JOIN lkp_data.lkp_state_details lk on ct.statee_cd=lk.state_cd)
SELECT potential_customer_cnt,population_cnt,statee_cd,count(DISTINCT(cust_id)) as count_per_date  from join_data GROUP BY statee_cd,potential_customer_cnt,population_cnt
ORDER BY statee_cd
