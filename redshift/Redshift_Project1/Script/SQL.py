query="select * from cards_ingest.tran_fact"
create_table='''create table cards_ingest.tran_fact2(
    tran_id int,
    cust_id varchar(10),
    stat_cd varchar(2),
    tran_ammt decimal(10,2),
    tran_date date,
    commison decimal(10,2)
)'''
count_query="select count(1) from cards_ingest.tran_fact"

count1_query='select count(1) from cards_ingest.tran_fact2'