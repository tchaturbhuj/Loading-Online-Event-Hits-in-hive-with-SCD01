use project1;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
insert overwrite table user_ext_part_01 partition (year, month) select b.custid,b.username,b.quote_count,b.ip,b.prp_1,b.prp_2,b.prp_3,b.ms,b.http_type,b.purchase_category,b.total_count,b.purchase_sub_category,b.http_info,b.status_code, cast(day(from_unixtime(unix_timestamp(b.entry_time , 'dd/MMM/yyyy'))) as int) as day, cast(year(from_unixtime(unix_timestamp(b.entry_time , 'dd/MMM/yyyy'))) as string) as year, cast(month(from_unixtime(unix_timestamp(b.entry_time , 'dd/MMM/yyyy'))) as string) as month from (select custid,max(lastmodified) as lastmodified 
        from user_managed_01 
        group by custid) a
left outer join user_managed_01 b on (a.custid=b.custid  and a.lastmodified=b.lastmodified)