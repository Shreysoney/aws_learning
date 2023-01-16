Create view cards_ingest1.vw_profit as
With valid_data as
(
SELECT bd.order_id,bd.brand_name,pd.product_name,pd.sales_ammount as
cost_ammount,bd.sales_ammount,(bd.sales_ammount-pd.sales_ammount) as profit ,bd.sales_date from cards_ingest.product_data pd
INNER JOIN cards_ingest.big_data bd on pd.product_name=bd.product_name),
pro_data as (SELECT order_id,brand_name,product_name,cost_ammount,sales_ammount,sales_date,profit,((profit/cost_ammount)*100) as profit_per from valid_data)
Select order_id,brand_name,product_name,cost_ammount,sales_ammount,sales_date,profit,profit_per,(case when profit_per>10 then 'Bumper Profit' when profit_per  BETWEEN 0 AND 10 then 'Marginal Profit'
when profit_per  BETWEEN -5 AND -1  then 'Lose Making'  else 'Bumper Loss' end) as profit_group from pro_data
