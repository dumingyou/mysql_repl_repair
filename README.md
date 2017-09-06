# mysql_repl_repair
mysql_repl_repair.py是一款用于修复mysql主从复制错误的python小工具，该工具可以修复由于主从数据不一致导致的1062(duplicate key), 1032(key not found)错误。当遇到复制出错，mysql_repl_repair.py会流式读取relay log中的数据，并构造成修复sql，在从库上执行，解决sql线程apply时遇到的问题。mysql_repl_repair.py非常轻巧，即使在遇到大事务时也不会对服务器造成性能影响，mysql_repl_repair.py支持以daemon方式后台运行，支持单机多实例下同时修复多个实例

目前网易内部的使用方法：监控服务定期监控mysql主从复制状态，如遇1062,1032 则执行mysql_repl_repair.py进行修复

 原理
======
1. 当从库sql apply线程遇到1062错误时，说明slave上已经存在需要insert的数据，并且需要insert的数据上有唯一约束，从而导致插入失败，那么需要按照 唯一约束建们 来删除该事务中相关insert语句(对应WRITE_ROWS_EVENT)。最终构造的sql是
```sql
delete from table where (pk_col = xxx ) or (uk1_col1 = xxx and uk1_col2=yyy)
```

如果事务中存在多条insert， 那么对应多条delete语句，而事务中有delete或者update的话，将忽略

2. 当从库sql apply线程遇到1032错误时，说明slave sql线程在执行update或者delete时找不到对应需要变更的数据，那么需要先写入这条数据才行，因为binlog为row模式时变更语句（对应DELETE_ROWS_EVENT或UPDATE_ROWS_EVENT）中包含变更前数据，因此可以构造出这条数据。最终构造的sql是
```sql
insert ignore into table set a=xxx,b=xxx,c=xxx
```
如果事务中包含多条delete/update语句，那么最终需要执行多次 insert操作，而事务中有insert的话，将忽略

 限制
======
* 支持5.1 ~ 5.7，
* 目前只支持ROW格式binlog且为FULL row image格式
* json，空间数据类型的表造成的复制异常目前版本暂不支持，如有强烈需求，我们将考虑支持一下

USAGE
===========
```shell
python mysql_repl_repair.py -h

Usage: 
python mysql_repl_repair.py [options]

this script is used to repair mysql replication errors(1062, 1032)

example:
python mysql_repl_repair.py -u mysql -p mysql -S /tmp/mysql.sock  -d -v
python mysql_repl_repair.py -u mysql -p mysql -S /tmp/mysql3306.sock,/tmp/mysql3307.sock -l /tmp


Options:
  -h, --help            show this help message and exit
  -u USER, --user=USER  username for login mysql
  -p PASSWORD, --password=PASSWORD
                        Password to use when connecting to server
  -l LOGDIR, --logdir=LOGDIR
                        log will output to screen by default,if run with
                        daemon mode, default logdir is /tmp, logfile is
                        $logdir/mysql_repl_repair.$port.log
  -S SOCKETS, --socket=SOCKETS
                        mysql sockets for connecting to server, you can input
                        multi socket to repair multi mysql instance, each
                        socket separate by ','
  -d, --daemon          run as a daemon
  -t TIME, --time=TIME  unit is second, default is 0 mean run forever
  -v, --verbose         debug log mode
  ```
 
  
  示例
================
1.授权本地用户权限,如果需要在多实例上同时执行，则每个实例都需要赋权
```sql
grant all on *.* to mysql@'localhost' identified by 'mysql';
```
2.执行脚本，注意：系统用户需要有读relay log的权限，没有的话用sudo
```shell
非debug日志模式：
sudo python /tmp/mysqlmon//mysql_repl_repair.py -u mysql -p mysql --socket=/tmp/mysql.sock
[INFO] [2017-09-05 20:18:55,279] [3306] ****************************************************************
[INFO] [2017-09-05 20:18:55,279] [3306]                          PROCESS START
[INFO] [2017-09-05 20:18:55,279] [3306] ****************************************************************
[INFO] [2017-09-05 20:18:55,280] [3306] ****************************************************************
[INFO] [2017-09-05 20:18:55,280] [3306]           REPL ERROR FOUND !!! STRAT REPAIR ERROR...
[INFO] [2017-09-05 20:18:55,280] [3306] ****************************************************************
[INFO] [2017-09-05 20:18:55,281] [3306] RELAYLOG FILE : /ebs/mysql_data/mysqld-relay-bin.000072 
[INFO] [2017-09-05 20:18:55,281] [3306] START POSITION : 259863 . STOP POSITION : 260223 
[INFO] [2017-09-05 20:18:55,281] [3306] ERROR MESSAGE : Could not execute Update_rows event on table dmy2.mytest; Can't find record in 'mytest', Error_code: 1032; handler error HA_ERR_KEY_NOT_FOUND; the event's master log mysql-bin.000008, end_log_pos 1000074349
[INFO] [2017-09-05 20:18:55,281] [3306] start parse relay log to fix this error...
[INFO] [2017-09-05 20:18:55,283] [3306] try to run this sql to resolve repl error, sql: insert ignore into `dmy2`.`mytest` set `a` = 101,`c` = 121,`b` = 10,`e` = x'746467736467',`d` = 5,`g` = x'6433797a736166',`f` = x'3433357479',`i` = '10:10:10.11',`h` = 10.22200,`k` = from_unixtime(1504612739.149),`j` = '1999-1-1',`m` = '1990',`l` = 555,`o` = 3,`n` = 1,`y` = 10.1999998093,`x` = 101.12
[INFO] [2017-09-05 20:18:55,388] [3306] slave repl error fixed success!
[INFO] [2017-09-05 20:18:57,390] [3306] SLAVE IS OK !, SKIP...
[INFO] [2017-09-05 20:18:59,393] [3306] SLAVE IS OK !, SKIP...

开启debug日志：
sudo python /tmp/mysqlmon//mysql_repl_repair.py -u mysql -p mysql --socket=/tmp/mysql3306.sock -v
[INFO] [2017-09-05 19:59:28,386] [3306] ****************************************************************
[INFO] [2017-09-05 19:59:28,386] [3306]                          PROCESS START
[INFO] [2017-09-05 19:59:28,386] [3306] ****************************************************************
[DEBUG] [2017-09-05 19:59:28,386] [3306] get file lock on /tmp/mysql_repl_repair3306.lck success
[DEBUG] [2017-09-05 19:59:28,387] [3306] start run sql: select @@datadir datadir
[DEBUG] [2017-09-05 19:59:28,387] [3306] sql result: {'datadir': '/ebs/mysql_data/'}
[DEBUG] [2017-09-05 19:59:28,387] [3306] start run sql: show slave status
[DEBUG] [2017-09-05 19:59:28,387] [3306] sql result: {'Replicate_Wild_Do_Table': '', 'Retrieved_Gtid_Set': '', 'Master_SSL_CA_Path': '', 'Last_Error': "Could not execute Update_rows event on table dmy2.mytest; Can't find record in 'mytest', Error_code: 1032; handler error HA_ERR_KEY_NOT_FOUND; the event's master log mysql-bin.000008, end_log_pos 999807692", 'Until_Log_File': '', 'SQL_Delay': 0L, 'Seconds_Behind_Master': None, 'Master_User': 'replicaUser', 'Master_Port': 3306L, 'Master_Retry_Count': 86400L, 'Until_Log_Pos': 0L, 'Master_Log_File': 'mysql-bin.000008', 'Read_Master_Log_Pos': 999814411L, 'Replicate_Do_DB': '', 'Master_SSL_Verify_Server_Cert': 'No', 'Exec_Master_Log_Pos': 999807332L, 'Replicate_Ignore_Server_Ids': '', 'Replicate_Ignore_Table': '', 'Master_Server_Id': 4787L, 'Relay_Log_Space': 3687797L, 'Last_SQL_Error': "Could not execute Update_rows event on table dmy2.mytest; Can't find record in 'mytest', Error_code: 1032; handler error HA_ERR_KEY_NOT_FOUND; the event's master log mysql-bin.000008, end_log_pos 999807692", 'SQL_Remaining_Delay': None, 'Relay_Master_Log_File': 'mysql-bin.000008', 'Master_SSL_Allowed': 'No', 'Master_SSL_CA_File': '', 'Slave_IO_State': 'Waiting for master to send event', 'Last_SQL_Error_Timestamp': '170905 19:58:59', 'Relay_Log_File': 'mysqld-relay-bin.000071', 'Replicate_Ignore_DB': '', 'Last_IO_Error': '', 'Until_Condition': 'None', 'Slave_SQL_Running_State': '', 'Replicate_Do_Table': '', 'Last_Errno': 1032L, 'Master_Host': '10.171.160.9', 'Master_Info_File': '/ebs/mysql_data/master.info', 'Master_SSL_Key': '', 'Executed_Gtid_Set': '', 'Master_Bind': '', 'Skip_Counter': 0L, 'Slave_SQL_Running': 'No', 'Relay_Log_Pos': 3667190L, 'Master_SSL_Cert': '', 'Last_IO_Errno': 0L, 'Slave_IO_Running': 'Yes', 'Connect_Retry': 60L, 'Last_SQL_Errno': 1032L, 'Last_IO_Error_Timestamp': '', 'Replicate_Wild_Ignore_Table': '', 'Master_UUID': 'b4dfc344-975c-11e6-addd-fa163e7f8534', 'Auto_Position': 0L, 'Master_SSL_Crl': '', 'Master_SSL_Cipher': '', 'Master_SSL_Crlpath': ''}
[INFO] [2017-09-05 19:59:28,387] [3306] ****************************************************************
[INFO] [2017-09-05 19:59:28,387] [3306]           REPL ERROR FOUND !!! STRAT REPAIR ERROR...
[INFO] [2017-09-05 19:59:28,387] [3306] ****************************************************************
[INFO] [2017-09-05 19:59:28,387] [3306] RELAYLOG FILE : /ebs/mysql_data/mysqld-relay-bin.000071 
[INFO] [2017-09-05 19:59:28,387] [3306] START POSITION : 3667190 . STOP POSITION : 3667550 
[INFO] [2017-09-05 19:59:28,387] [3306] ERROR MESSAGE : Could not execute Update_rows event on table dmy2.mytest; Can't find record in 'mytest', Error_code: 1032; handler error HA_ERR_KEY_NOT_FOUND; the event's master log mysql-bin.000008, end_log_pos 999807692
[INFO] [2017-09-05 19:59:28,387] [3306] start parse relay log to fix this error...
[DEBUG] [2017-09-05 19:59:28,389] [3306] read column for dmy2.mytest, column: a, data type: int, read bytes 4, column value: 101
[DEBUG] [2017-09-05 19:59:28,389] [3306] read column for dmy2.mytest, column: b, data type: smallint, read bytes 2, column value: 10
[DEBUG] [2017-09-05 19:59:28,389] [3306] read column for dmy2.mytest, column: c, data type: int, read bytes 4, column value: 121
[DEBUG] [2017-09-05 19:59:28,389] [3306] read column for dmy2.mytest, column: d, data type: mediumint, read bytes 3, column value: 1110111
[DEBUG] [2017-09-05 19:59:28,389] [3306] read column for dmy2.mytest, column: y, data type: float, read bytes 4, column value: 10.1999998093
[DEBUG] [2017-09-05 19:59:28,389] [3306] read column for dmy2.mytest, column: x, data type: double, read bytes 8, column value: 101.12
[DEBUG] [2017-09-05 19:59:28,389] [3306] read column for dmy2.mytest, column: e, data type: varchar, read bytes 8, column value: x'746467736467'
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: f, data type: char, read bytes 6, column value: x'3433357479'
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: g, data type: text, read bytes 9, column value: x'6433797a736166'
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: h, data type: decimal, read bytes 6, column value: 10.22200
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: i, data type: time, read bytes 4, column value: '10:10:10.11'
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: j, data type: date, read bytes 3, column value: '1999-1-1'
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: k, data type: timestamp, read bytes 6, column value: from_unixtime(1504239488.348)
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: l, data type: bigint, read bytes 8, column value: 555
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: m, data type: year, read bytes 1, column value: '1990'
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: n, data type: enum, read bytes 1, column value: 1
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: o, data type: set, read bytes 1, column value: 3
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: a, data type: int, read bytes 4, column value: 101
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: b, data type: smallint, read bytes 2, column value: 10
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: c, data type: int, read bytes 4, column value: 121
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: d, data type: mediumint, read bytes 3, column value: 5
[DEBUG] [2017-09-05 19:59:28,390] [3306] read column for dmy2.mytest, column: y, data type: float, read bytes 4, column value: 10.1999998093
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: x, data type: double, read bytes 8, column value: 101.12
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: e, data type: varchar, read bytes 8, column value: x'746467736467'
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: f, data type: char, read bytes 6, column value: x'3433357479'
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: g, data type: text, read bytes 9, column value: x'6433797a736166'
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: h, data type: decimal, read bytes 6, column value: 10.22200
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: i, data type: time, read bytes 4, column value: '10:10:10.11'
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: j, data type: date, read bytes 3, column value: '1999-1-1'
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: k, data type: timestamp, read bytes 6, column value: from_unixtime(1504612739.149)
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: l, data type: bigint, read bytes 8, column value: 555
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: m, data type: year, read bytes 1, column value: '1990'
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: n, data type: enum, read bytes 1, column value: 1
[DEBUG] [2017-09-05 19:59:28,391] [3306] read column for dmy2.mytest, column: o, data type: set, read bytes 1, column value: 3
[DEBUG] [2017-09-05 19:59:28,391] [3306] filename: /ebs/mysql_data/mysqld-relay-bin.000071,start_pos: 3667546,rowdata: {'table_name': 'mytest', 'table_schema': 'dmy2', 'data': {'a': 101, 'c': 121, 'b': 10, 'e': "x'746467736467'", 'd': 1110111, 'g': "x'6433797a736166'", 'f': "x'3433357479'", 'i': "'10:10:10.11'", 'h': Decimal('10.22200'), 'k': 'from_unixtime(1504239488.348)', 'j': "'1999-1-1'", 'm': "'1990'", 'l': 555, 'o': 3, 'n': 1, 'y': 10.199999809265137, 'x': 101.12}, 'event_type': 31, 'data2': {'a': 101, 'c': 121, 'b': 10, 'e': "x'746467736467'", 'd': 5, 'g': "x'6433797a736166'", 'f': "x'3433357479'", 'i': "'10:10:10.11'", 'h': Decimal('10.22200'), 'k': 'from_unixtime(1504612739.149)', 'j': "'1999-1-1'", 'm': "'1990'", 'l': 555, 'o': 3, 'n': 1, 'y': 10.199999809265137, 'x': 101.12}}
[INFO] [2017-09-05 19:59:28,392] [3306] try to run this sql to resolve repl error, sql: insert ignore into `dmy2`.`mytest` set `a` = 101,`c` = 121,`b` = 10,`e` = x'746467736467',`d` = 1110111,`g` = x'6433797a736166',`f` = x'3433357479',`i` = '10:10:10.11',`h` = 10.22200,`k` = from_unixtime(1504239488.348),`j` = '1999-1-1',`m` = '1990',`l` = 555,`o` = 3,`n` = 1,`y` = 10.1999998093,`x` = 101.12
[DEBUG] [2017-09-05 19:59:28,392] [3306] start run sql: insert ignore into `dmy2`.`mytest` set `a` = 101,`c` = 121,`b` = 10,`e` = x'746467736467',`d` = 1110111,`g` = x'6433797a736166',`f` = x'3433357479',`i` = '10:10:10.11',`h` = 10.22200,`k` = from_unixtime(1504239488.348),`j` = '1999-1-1',`m` = '1990',`l` = 555,`o` = 3,`n` = 1,`y` = 10.1999998093,`x` = 101.12
[DEBUG] [2017-09-05 19:59:28,393] [3306] sql result: None
[DEBUG] [2017-09-05 19:59:28,393] [3306] start run sql: stop slave;
[DEBUG] [2017-09-05 19:59:28,429] [3306] sql result: None
[DEBUG] [2017-09-05 19:59:28,429] [3306] start run sql: start slave
[DEBUG] [2017-09-05 19:59:28,445] [3306] sql result: None
[DEBUG] [2017-09-05 19:59:28,545] [3306] start run sql: show slave status
[DEBUG] [2017-09-05 19:59:28,553] [3306] sql result: {'Replicate_Wild_Do_Table': '', 'Retrieved_Gtid_Set': '', 'Master_SSL_CA_Path': '', 'Last_Error': '', 'Until_Log_File': '', 'SQL_Delay': 0L, 'Seconds_Behind_Master': 24L, 'Master_User': 'replicaUser', 'Master_Port': 3306L, 'Master_Retry_Count': 86400L, 'Until_Log_Pos': 0L, 'Master_Log_File': 'mysql-bin.000008', 'Read_Master_Log_Pos': 999814411L, 'Replicate_Do_DB': '', 'Master_SSL_Verify_Server_Cert': 'No', 'Exec_Master_Log_Pos': 999809186L, 'Replicate_Ignore_Server_Ids': '', 'Replicate_Ignore_Table': '', 'Master_Server_Id': 4787L, 'Relay_Log_Space': 3688136L, 'Last_SQL_Error': '', 'SQL_Remaining_Delay': None, 'Relay_Master_Log_File': 'mysql-bin.000008', 'Master_SSL_Allowed': 'No', 'Master_SSL_CA_File': '', 'Slave_IO_State': 'Waiting for master to send event', 'Last_SQL_Error_Timestamp': '', 'Relay_Log_File': 'mysqld-relay-bin.000071', 'Replicate_Ignore_DB': '', 'Last_IO_Error': '', 'Until_Condition': 'None', 'Slave_SQL_Running_State': 'System lock', 'Replicate_Do_Table': '', 'Last_Errno': 0L, 'Master_Host': '10.171.160.9', 'Master_Info_File': '/ebs/mysql_data/master.info', 'Master_SSL_Key': '', 'Executed_Gtid_Set': '', 'Master_Bind': '', 'Skip_Counter': 0L, 'Slave_SQL_Running': 'Yes', 'Relay_Log_Pos': 3669044L, 'Master_SSL_Cert': '', 'Last_IO_Errno': 0L, 'Slave_IO_Running': 'Yes', 'Connect_Retry': 60L, 'Last_SQL_Errno': 0L, 'Last_IO_Error_Timestamp': '', 'Replicate_Wild_Ignore_Table': '', 'Master_UUID': 'b4dfc344-975c-11e6-addd-fa163e7f8534', 'Auto_Position': 0L, 'Master_SSL_Crl': '', 'Master_SSL_Cipher': '', 'Master_SSL_Crlpath': ''}
[INFO] [2017-09-05 19:59:28,553] [3306] slave repl error fixed success!
[DEBUG] [2017-09-05 19:59:30,555] [3306] start run sql: show slave status
[DEBUG] [2017-09-05 19:59:30,556] [3306] sql result: {'Replicate_Wild_Do_Table': '', 'Retrieved_Gtid_Set': '', 'Master_SSL_CA_Path': '', 'Last_Error': '', 'Until_Log_File': '', 'SQL_Delay': 0L, 'Seconds_Behind_Master': 0L, 'Master_User': 'replicaUser', 'Master_Port': 3306L, 'Master_Retry_Count': 86400L, 'Until_Log_Pos': 0L, 'Master_Log_File': 'mysql-bin.000008', 'Read_Master_Log_Pos': 999814829L, 'Replicate_Do_DB': '', 'Master_SSL_Verify_Server_Cert': 'No', 'Exec_Master_Log_Pos': 999814829L, 'Replicate_Ignore_Server_Ids': '', 'Replicate_Ignore_Table': '', 'Master_Server_Id': 4787L, 'Relay_Log_Space': 3675026L, 'Last_SQL_Error': '', 'SQL_Remaining_Delay': None, 'Relay_Master_Log_File': 'mysql-bin.000008', 'Master_SSL_Allowed': 'No', 'Master_SSL_CA_File': '', 'Slave_IO_State': 'Waiting for master to send event', 'Last_SQL_Error_Timestamp': '', 'Relay_Log_File': 'mysqld-relay-bin.000072', 'Replicate_Ignore_DB': '', 'Last_IO_Error': '', 'Until_Condition': 'None', 'Slave_SQL_Running_State': 'Slave has read all relay log; waiting for the slave I/O thread to update it', 'Replicate_Do_Table': '', 'Last_Errno': 0L, 'Master_Host': '10.171.160.9', 'Master_Info_File': '/ebs/mysql_data/master.info', 'Master_SSL_Key': '', 'Executed_Gtid_Set': '', 'Master_Bind': '', 'Skip_Counter': 0L, 'Slave_SQL_Running': 'Yes', 'Relay_Log_Pos': 703L, 'Master_SSL_Cert': '', 'Last_IO_Errno': 0L, 'Slave_IO_Running': 'Yes', 'Connect_Retry': 60L, 'Last_SQL_Errno': 0L, 'Last_IO_Error_Timestamp': '', 'Replicate_Wild_Ignore_Table': '', 'Master_UUID': 'b4dfc344-975c-11e6-addd-fa163e7f8534', 'Auto_Position': 0L, 'Master_SSL_Crl': '', 'Master_SSL_Cipher': '', 'Master_SSL_Crlpath': ''}
[INFO] [2017-09-05 19:59:30,556] [3306] SLAVE IS OK !, SKIP...
^CBye.Bye
```

3.结束：
如果不是以daemon方式运行，那么只需要Ctrl+C即可结束，如果是以daemon方式，直接kill进程即可

感谢
======
代码大部分解析binlog的函数代码都是参考https://github.com/noplay/python-mysql-replication
在此非常感谢python-mysql-replication的作者们的付出

作者
=====
杜明友、赵天元


问题反馈方式
================

* qq群: 116121252
* email: dukope@163.com,tianyuanzhao@126.com
