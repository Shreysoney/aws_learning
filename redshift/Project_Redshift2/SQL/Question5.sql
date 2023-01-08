with proc_data as (select tran_id,cust_id,coalesce(stat_cd,case when cust_id='cust_109' then 'TX' else 'CA' end) as statee_cd ,tran_ammt,tran_date from cards_ingest.tran_fact),
join_data as (select * from proc_data ct INNER JOIN lkp_data.lkp_state_details lk on ct.statee_cd=lk.state_cd)
SELECT potential_customer_cnt,population_cnt,statee_cd,count(DISTINCT(cust_id)) as count_per_date  from join_data GROUP BY statee_cd,potential_customer_cnt,population_cnt
ORDER BY statee_cd
