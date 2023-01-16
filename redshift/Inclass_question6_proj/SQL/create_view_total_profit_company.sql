
Create view cards_ingest1.vw_total_profit_per_company_month as
With valid_data as
(
SELECT bd.order_id,bd.brand_name,pd.product_name,pd.sales_ammount as
cost_ammount,bd.sales_ammount,(bd.sales_ammount-pd.sales_ammount) as profit ,bd.sales_date from cards_ingest.product_data pd
INNER JOIN cards_ingest.big_data bd on pd.product_name=bd.product_name),
pro_data as (SELECT order_id,brand_name,product_name,cost_ammount,sales_ammount,EXTRACT(YEAR from sales_date) as sales_year,EXTRACT(MONTH from sales_date) as sales_month,sales_date,profit from valid_data),
final_data as (SELECT brand_name,sales_year,sales_month,sum(profit) as total_profit,count(*) as total_products from pro_data GROUP by brand_name,sales_year,sales_month order by sales_year,sales_month)
select * ,dense_rank()over(partition by sales_year,sales_month order by total_profit desc ,total_products asc) from final_data order by sales_year,sales_month
