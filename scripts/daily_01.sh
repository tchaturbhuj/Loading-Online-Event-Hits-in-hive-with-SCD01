#!/bin/sh

#sourcing the parzmeter file
. /home/saif/cohort_FF11/env/project.prm
. /home/saif/cohort_FF11/env/check_files/incremental_01.prm

#specifing log file details
LOG_DIR=/home/saif/cohort_FF11/logs/project/
LFILE_NAME=`basename $0`
DT=`date '+%Y%m%d_%H:%M:%S'`
LOG_FILE_NAME=${LOG_DIR}${LFILE_NAME}_${DT}.log
echo "" > $LOG_FILE_NAME

#checking for new file
flag=0
yourfilenames=`ls /home/saif/cohort_FF11/archive_01`
for eachfile in $yourfilenames
do
   data=`grep -w $eachfile /home/saif/cohort_FF11/env/check_files/check_01.txt`
   if [ -z "$data" ]
   then
   echo "$eachfile is new file"
   echo "$eachfile is new file" >> $LOG_FILE_NAME
   flag=1
   f_data=$eachfile
   else
   echo "$eachfile Already Loaded"
   echo "$eachfile Already Loaded" >> $LOG_FILE_NAME
   fi
done


#if there is no new file program will not run
if [ $flag = 0 ]
then
echo "All files are already loaded" >> $LOG_FILE_NAME
exit 1
fi

#adding data into tables
mysql --local-infile=1 -u "${USERNAME}" -p"${PASSWORD}" "${DB_NAME}" <<EOF
  	use ${DB_NAME};
  	SET GLOBAL local_infile=1;
  	truncate table user_data_01;
	LOAD DATA LOCAL INFILE '/home/saif/cohort_FF11/archive_01/${f_data}'
	INTO TABLE user_data_01
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n';
	update user_data_01 set lastModified=now();
	truncate table user_data_reconciallation_01;
EOF
if [ $? = 0 ]
then
echo "Data got successfully loaded in mysql database" >> $LOG_FILE_NAME
echo "Data got successfully loaded in mysql database"
else
echo "Data load in mysql database failed" >> $LOG_FILE_NAME
echo "Data load in mysql database failed"
exit 1
fi

#running incremental job for sending data to hdfs
sqoop job -exec job_user_incremental_01

if [ $? = 0 ]
then
echo "Data got successfully injected in hdfs with incremental load" >> $LOG_FILE_NAME
echo "Data got successfully injected in hdfs with incremental load"
else
echo "Data injectation in hdfs with incremental load got failed" >> $LOG_FILE_NAME
echo "Data injectation in hdfs with incremental load got failed"
exit 1
fi

#copying data in one temporary directory, becuase creating managed table deleted files
hadoop fs -mkdir /user/saif/HFS/output/temp_copy
hadoop fs -cp /user/saif/HFS/output/user_data_01/* /user/saif/HFS/output/temp_copy

#sending data to reconciallation
hive --hiveconf last_mod="${LASTMODIFIED}" -f /home/saif/cohort_FF11/env/project_incremental_01.hql

#after creating managed table copying again files to its loacatio
hadoop fs -cp /user/saif/HFS/output/temp_copy/* /user/saif/HFS/output/user_data_01
hadoop fs -rm -r /user/saif/HFS/output/temp_copy

if [ $? = 0 ]
then
echo "Data got successfully send to user_managed, user_reconciallation_managed table in hive" >> $LOG_FILE_NAME
echo "Data got successfully send to user_managed, user_reconciallation_managed table in hive"
else
echo "Data sending to user_managed, user_reconciallation_managed table in hive got failed" >> $LOG_FILE_NAME
echo "Data sending to user_managed, user_reconciallation_managed table in hive got failed"
exit 1
fi

#updating incremental prm file
echo "LASTMODIFIED='`date '+%Y-%m-%d %H:%M:%S'`'" > /home/saif/cohort_FF11/env/check_files/incremental_01.prm

hive -f /home/saif/cohort_FF11/env/project_daily_01.hql

if [ $? = 0 ]
then
echo "Data got successfully send to user_ext_part table in hive with partition by year, month" >> $LOG_FILE_NAME
echo "Data got successfully send to user_ext_part table in hive with partition by year, month"
else
echo "Data sending to user_ext_part table in hive with partition by year, month got failed" >> $LOG_FILE_NAME
echo "Data sending to user_ext_part table in hive with partition by year, month got failed"
exit 1
fi

sqoop export \
--connect jdbc:mysql://${HOST}:${PORT_NO}/${DB_NAME}?useSSL=False \
--username ${USERNAME} \
--password-file ${PASSWORD_FILE} \
--table user_data_reconciallation_01 \
--export-dir /user/hive/warehouse/project1.db/user_reconciallation_managed_01 \
--input-fields-terminated-by ','

if [ $? = 0 ]
then
echo "Data got successfully exported in user_data_reconciallation table in sql" >> $LOG_FILE_NAME
echo "Data got successfully exported in user_data_reconciallation table in sql"
else
echo "Data exporting to user_data_reconciallation table in sql got failed" >> $LOG_FILE_NAME
echo "Data exporting to user_data_reconciallation table in sql got failed"
exit 1
fi


#checking reconciallation table have successful loaded same values
count=`mysql -u "${USERNAME}" -p"${PASSWORD}" "${DB_NAME}" -N -e 'SELECT count(custid) FROM user_data_01
where md5(concat(custid,username,quote_count,ip,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code)) not in
(SELECT md5(concat(custid,username,quote_count,ip,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,purchase_sub_category,http_info,status_code)) 
as checksum_user_data_reconciallation FROM user_data_reconciallation_01);'`

if [ $count = 0 ]
then
echo "All the data is successfully loaded and reconciallation was successfull"
echo $f_data >> /home/saif/cohort_FF11/env/check_files/check_01.txt
else
echo "Data reconciallation failed, so something wrong"
fi



