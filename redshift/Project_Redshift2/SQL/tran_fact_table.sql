create table if not exists cards_ingest.tran_fact(
    tran_id int ,
    cust_id varchar(10), 
    stat_cd varchar(2) , 
    tran_ammt decimal(10,2) ,
    tran_date date 
);
