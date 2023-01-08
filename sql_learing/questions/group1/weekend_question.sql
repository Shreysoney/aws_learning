-- Questions:

-- Part 1
-- 1. show me all the tran_date,tran_ammt and total transaction ammount per tran_date


select dt.tran_ammt,dt.tran_date,sum(dt.tran_ammt) over(partition by dt.tran_date)  from cards_ingest.tran_fact dt order by dt.tran_date,dt.tran_ammt



-- 2. show me all the tran_date,tran_ammt and total transaction ammount per tran_date and rank of the transaction ammount desc within per tran_date
/*
Ouput:
2022-01-01,7145.00,19543.00,1
2022-01-01,6125.00,19543.00,2
*/
select dt.tran_ammt,dt.tran_date,sum(dt.tran_ammt) over(partition by dt.tran_date), rank() over(partition by dt.tran_date order by dt.tran_ammt desc) from cards_ingest.tran_fact dt 



-- 3. show me all the fields and total tansaction ammount per tran_date and only 2nd rank of the transaction ammount desc within per tran_date
--  (Here you are using he question2 but filtering only for rank 2)
select * from (select *,dt.tran_ammt,dt.tran_date,sum(dt.tran_ammt) over(partition by dt.tran_date), rank() over(partition by dt.tran_date order by dt.tran_ammt desc) as rnk  from cards_ingest.tran_fact dt ) inq where inq.rnk=2 



-- Part 2

-- 1. Join tran_fact and cust_dim_details on cust_id and tran_dt between start_date and end_date
select * from (select *,dt.tran_ammt,dt.tran_date,sum(dt.tran_ammt) over(partition by dt.tran_date), rank() over(partition by dt.tran_date order by dt.tran_ammt desc) as rnk  from cards_ingest.tran_fact dt ) inq where inq.rnk=2


-- 2. show me all the fields and total tansaction ammount per tran_date and only 2nd rank of the transaction
--  ammount desc within per tran_date(Here you are using he question2 but filtering only for rank 2) and join
--   cust_dim_details on cust_id and tran_dt between start_date and end_date
select * from (select *,inq1.tran_ammt,inq1.tran_date,sum(inq1.tran_ammt) over(partition by inq1.tran_date), rank() over(partition by inq1.tran_date order by inq1.tran_ammt desc) as rnk  from (select * from cards_ingest.tran_fact dt inner join cards_ingest.cust_dim_details ft on dt.cust_id=ft.cust_id where dt.tran_date between ft.start_date and ft.end_date) inq1 ) inq where inq.rnk=2 


-- 3. From question 2 : when stat_cd is not euqal to state_cd then data issues else good data as stae_cd_status
--  [Note NUll from left side is not equal NUll from other side  >> means lets sayd NULL value from fact table if compared
--  to NULL Value to right table then it should be data issues]

select * from (select *,inq1.tran_ammt,inq1.tran_date,sum(inq1.tran_ammt) over(partition by inq1.tran_date), rank() over(partition by inq1.tran_date order by inq1.tran_ammt desc) as rnk  from (select *,case when dt.stat_cd=ft.state_cd then 'good data' else 'data issues'end as stae_cd_status from cards_ingest.tran_fact dt inner join cards_ingest.cust_dim_details ft on dt.cust_id=ft.cust_id where dt.tran_date between ft.start_date and ft.end_date) inq1 ) inq 