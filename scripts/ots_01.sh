#!/bin/sh

#sourcing the parzmeter file
. /home/saif/cohort_FF11/env/project.prm

#connecting to database
mysql --local-infile=1 -u "${USERNAME}" -p"${PASSWORD}" "${DB_NAME}" <<EOF
  use ${DB_NAME};
  
  #creating user_data table in sql
  create table user_data_01(
	custid integer(10),
	username varchar(30),
	quote_count varchar(30),
	ip varchar(30),
	entry_time varchar(30),
	prp_1 varchar(30),
	prp_2 varchar(30),
	prp_3 varchar(30),
	ms varchar(30),
	http_type varchar(30),
	purchase_category varchar(30),
	total_count varchar(30),
	purchase_sub_category varchar(30),
	http_info varchar(30),
	status_code integer(10),
	lastModified datetime
	);
  
  #creating user_data_reconciallation table in sql
  create table user_data_reconciallation_01(
	custid integer(10),
	username varchar(30),
	quote_count varchar(30),
	ip varchar(30),
	entry_time varchar(30),
	prp_1 varchar(30),
	prp_2 varchar(30),
	prp_3 varchar(30),
	ms varchar(30),
	http_type varchar(30),
	purchase_category varchar(30),
	total_count varchar(30),
	purchase_sub_category varchar(30),
	http_info varchar(30),
	status_code varchar(30),
	lastModified varchar(30)
	);


EOF


 

#creating sqoop job for incremental operation
sqoop job \
--create job_user_incremental_01 \
-- import \
--connect jdbc:mysql://localhost:3306/project1?useSSL=False \
--username root \
--password-file file:///home/saif/cohort_FF11/datasets/sqoop.pwd \
--query 'select custid,username,quote_count,ip,entry_time,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code,lastModified from user_data_01 where $CONDITIONS' \
--target-dir /user/saif/HFS/output/user_data_01 \
--m 1 \
--incremental append \
--check-column lastmodified \
--last-value '1900-01-01 00:00:00'

#creating lastmodified to check new data
echo LASTMODIFIED="'1900-01-01 00:00:00'" > /home/saif/cohort_FF11/env/check_files/incremental_01.prm
touch /home/saif/cohort_FF11/env/check_files/check_01.txt

#calling hql file to load data and create table on hive
hive -f '/home/saif/cohort_FF11/env/project_create_01.hql'





