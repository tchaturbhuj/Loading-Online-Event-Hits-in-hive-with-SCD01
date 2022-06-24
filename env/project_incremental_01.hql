use project1;
set last_mod; 
set hiveconf:last_mod;
load data inpath '/user/saif/HFS/output/user_data_01/*' overwrite into table user_managed_01;
insert overwrite table user_reconciallation_managed_01 select custid,username,quote_count,ip,entry_time,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code,lastModified from user_managed_01 where unix_timestamp(lastmodified, 'yyyy-MM-dd HH:mm:ss') > unix_timestamp('${hiveconf:last_mod}' , 'yyyy-MM-dd HH:mm:ss');