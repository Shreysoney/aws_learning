unload ('select * from cards_ingest1.vw_procost_data') to
's3://tranformdata/vw_procost/'
iam_role'arn:aws:iam::781214945837:role/redshifts3'
csv parallel off allowoverwrite DELIMITER '|' header;