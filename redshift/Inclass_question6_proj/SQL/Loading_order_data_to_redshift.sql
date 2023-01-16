copy cards_ingest.big_data from 's3://redshifts3demo/order_data_20230401.csv'
iam_role 'arn:aws:iam::781214945837:role/redshifts3'
csv;