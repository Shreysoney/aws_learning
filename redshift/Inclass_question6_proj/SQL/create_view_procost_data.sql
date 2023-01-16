create view cards_ingest1.vw_procost_data AS
SELECT bd.order_id,bd.brand_name,pd.product_name,pd.sales_ammount as
cost_ammount,bd.sales_ammount,bd.sales_date from cards_ingest.product_data pd
INNER JOIN cards_ingest.big_data bd on pd.product_name=bd.product_name;