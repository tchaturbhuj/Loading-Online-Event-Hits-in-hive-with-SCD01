# Project1 : Loading Online Event Hits using Sqoop to Hive via Shell Script
 Creating a script to automate data load in HDFS with reconciliation of data
 

![WhatsApp Image 2022-06-17 at 4 51 17 PM](https://user-images.githubusercontent.com/107996635/176876458-64cb4fa5-95d1-48fa-9721-18e701317dea.jpeg)



1. At first I have written a script to automate reading of new files from archive folder and transferred data in MySQL.
2. Created a Sqoop Job which will use Sqoop Incremental Operation to send data in HDFS (Hadoop File System).
3. Created Managed Table in Hive and then transferred data from HDFS to hive table.
4. Then Created External table with dynamic partition on bases of (year, month) and load data into external table from managed table with SCD01 Logic.
5. At last exported updated data from hive table to SQL for data reconciliation.
