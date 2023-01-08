with proc_data as (select tran_id,cust_id,coalesce(stat_cd,case when cust_id='cust_109' then 'TX' else 'CA' end) as statee_cd ,tran_ammt,tran_date from cards_ingest.tran_fact),
join_data as (select * from proc_data ct INNER JOIN lkp_data.lkp_state_details lk on ct.statee_cd=lk.state_cd),
count_data as(SELECT potential_customer_cnt,tran_date,statee_cd,count(DISTINCT(cust_id)) as count_per_date  from join_data GROUP BY tran_date,statee_cd,potential_customer_cnt
ORDER BY tran_date,statee_cd),
proce_data as(SELECT tran_date,statee_cd ,(potential_customer_cnt-count_per_date) as countt,(potential_customer_cnt-count_per_date)*5 as valuee from count_data ORDER BY tran_date,statee_cd),
all_data as (SELECT tran_date,statee_cd,countt,valuee,rank()over(PARTITION BY tran_date ORDER BY valuee DESC) as rank_data from proce_data order by tran_date,valuee)
SELECT * from all_data where rank_data=2
