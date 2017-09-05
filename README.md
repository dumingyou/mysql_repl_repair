# mysql_repl_repair
this script is used to repair mysql replication errors(1062, 1032)

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
  
  特别说明：
  =========
  *支持5.1 ~ 5.7，如有版本不支持或者出错，请联系我，我们将进行修复
  *目前只支持ROW格式binlog且为FULL row image格式
  *json，空间数据类型的表造成的复制异常目前版本暂不支持，如有需求请提出，我们将考虑支持一下
  *代码大部分解析binlog的函数代码都是参考https://github.com/noplay/python-mysql-replication，非常感谢python-mysql-replication的作者们的付出
  
  
  
  
  问题反馈方式：
  ============
  *qq: 137727431
  *email: dukope@163.com
