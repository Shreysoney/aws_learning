unload ('select * from cards_ingest1.vw_total_profit_per_company_month') to
's3://tranformdata/vw_profit_company_rnk/'
iam_role'arn:aws:iam::781214945837:role/redshifts3'
csv parallel off  DELIMITER '|' header;