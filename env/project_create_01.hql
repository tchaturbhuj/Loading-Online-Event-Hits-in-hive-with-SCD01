use project1;

create table if not exists user_managed_01
(
custid int,
username string,
quote_count string,
ip string,
entry_time string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code string,
lastModified string
)
row format delimited fields terminated by ',';

create table if not exists user_reconciallation_managed_01
(
custid int,
username string,
quote_count string,
ip string,
entry_time string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code int,
lastModified string
)
row format delimited fields terminated by ',';

create external table if not exists user_ext_part_01
(
custid int,
username string,
quote_count string,
ip string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code int,
day int
)
partitioned by (year string,month string)
row format delimited fields terminated by ',';