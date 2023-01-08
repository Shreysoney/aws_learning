with join_data as (select * from cards_ingest.tran_fact ct INNER JOIN lkp_data.lkp_state_details lk on ct.stat_cd=lk.state_cd ),
proc_data as(SELECT cust_id,stat_cd,tran_date,population_cnt,row_number()over(partition by cust_id,stat_cd order by tran_date) as rnk from join_data order by stat_cd,tran_date),
valid_data as(select *,(case when rnk=1 then rnk else 0 end) as cnt from proc_data),
all_data as (select cust_id,stat_cd,tran_date,sum(cnt)over(partition by stat_cd rows unbounded preceding ) as unique_running_state_cust from valid_data),
final_data as(select * , coalesce(lag(unique_running_state_cust,1)over(partition by stat_cd order by tran_date),0)as prev from all_data)
select cust_id,stat_cd, tran_date,unique_running_state_cust,(case when unique_running_state_cust=prev then 0 else unique_running_state_cust end) as unique_state_customers from final_data


