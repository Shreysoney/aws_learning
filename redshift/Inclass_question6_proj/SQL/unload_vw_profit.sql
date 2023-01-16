unload ('select * from cards_ingest1.vw_profit') to
's3://tranformdata/vw_profit_grp/'
iam_role'arn:aws:iam::781214945837:role/redshifts3'
csv parallel off  DELIMITER '|' header;