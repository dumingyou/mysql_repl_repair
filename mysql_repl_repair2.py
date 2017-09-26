#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author: dumingyou,zhaotianyuan from netease
#this script is used to repair mysql replication errors(1062, 1032)
#need install pymysqlreplication first
#pymysqlreplication: https://github.com/noplay/python-mysql-replication
#1062: HA_ERR_FOUND_DUPP_KEY (duplicate key)
#1032: HA_ERR_KEY_NOT_FOUND (key not found)

import MySQLdb.cursors
import datetime
import sys,os,fcntl,time,decimal,struct,signal,logging
from optparse import OptionParser
from threading import Thread


try:
	from pymysqlreplication import BinLogStreamReader
	from pymysqlreplication.row_event import (
		DeleteRowsEvent,
		UpdateRowsEvent,
		WriteRowsEvent,
		TableMapEvent
	)
except Exception, e:
	print "please install pymysqlreplication first\n"\
	"github: https://github.com/noplay/python-mysql-replication"
	sys.exit()


sigint_up = False

def usage():
	"Print usage and parse input variables"

	usage = "\n"
	usage += "python " + sys.argv[0] + " [options]\n"
	usage += "\n"
	usage += "this script is used to repair mysql replication errors(1062, 1032)\n"
	usage += "\n"
	usage += "example:\n"
	usage += "python %s -i 192.168.1.1:3306  -u mysql -p mysql -v\n" %(sys.argv[0])
	usage += "python %s -i 192.168.1.1:3306,192.168.1.2:3306 -u mysql -p mysql -d -l tmp\n" %(sys.argv[0])

	parser = OptionParser(usage)

	parser.add_option("-u", "--user", dest="user", action="store",
			help = "username for login mysql instance and its master")

	parser.add_option("-p", "--password", dest="password", action="store",
			help = "Password to use when connecting to mysql instance and its master")

	parser.add_option("-l", "--logdir", dest="logdir", action="store",
			  help = "log will output to screen by default,"\
				  "if run with daemon mode, default logdir is /tmp,"\
				  " logfile is $logdir/mysql_repl_repair.$port.log")

	parser.add_option("-i", "--instances",dest="instances", action="store",
			help = "mysql instances which need repair, separate by ','."\
			" it will repair all instances store in config file if this option not set")

	parser.add_option("-d", "--daemon",dest="daemon", action="store_true",
			default=False, help = "run as a daemon")

	parser.add_option("-t", "--time",dest="time", action="store",
			default='0', help = "unit is second, default is 0 mean run forever")

	parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
			default=False, help = "debug log mode")

	(options,args)=parser.parse_args()

	if not options.time.isdigit():
		print "please input a integer value for time"
		sys.exit()

	if options.daemon and options.logdir is None:
		options.logdir = "/tmp"

	if options.logdir is not None:
		if not os.path.exists(options.logdir):
			print "logdir %s is not exists" %(options.logdir)
			sys.exit()

	if not( options.user and options.password and options.instances):
		print "please input user password instances"
		sys.exit()
	return options

class Daemon(object):
	"A generic daemon class"

	def daemonize(self):

		# do first fork
		try:
			pid = os.fork()

			if pid > 0:
				# exit first parent
				sys.exit(0)

		except OSError, e:
			sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
			sys.exit(1)

		# decouple from parent environment
		os.chdir("/")
		os.setsid()
		os.umask(0)

		# do second fork
		try:
			pid = os.fork()
			if pid > 0:
				# exit from second parent
				sys.exit(0)

		except OSError, e:
			sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
			sys.exit(1)

		# redirect standard file descriptors
		sys.stdout.flush()
		sys.stderr.flush()
		si = file(os.devnull, 'r')
		so = file(os.devnull, 'a+')
		se = file(os.devnull, 'a+')
		os.dup2(si.fileno(), sys.stdin.fileno())
		os.dup2(so.fileno(), sys.stdout.fileno())
		os.dup2(se.fileno(), sys.stderr.fileno())

	def start(self):
		"Start the daemon"

		# Start the daemon
		self.daemonize()
		self.run()

	def run(self):
		"You should override this method when you subclass Daemon"

def sigint_handler(signum, frame):
	global sigint_up
	sigint_up = True

class MysqlReplRepairDaemon(Daemon):
	"run mysql repl repair as a daemon"

	def __init__(self, option):
		self.option = option

	def run(self):
		run_mysql_repl_repair(self.option)

def run_mysql_repl_repair(op):

	threadlist = []
	runseconds = int(op.time) - 1

	for instance in op.instances.split(","):
		ip,port = instance.split(":")
		myrepair = MysqlReplRepair(op.user, op.password, ip, port, op.logdir, op.verbose)
		threadlist.append(myrepair)

	for thread in threadlist:
		thread.start()

	while runseconds !=0 :
		if sigint_up:
			print "Bye.Bye"
			break
		time.sleep(1)
		runseconds = runseconds -1

	for thread in threadlist:
		thread._Thread__stop()

class MysqlReplRepair(Thread):
	"Do MySQL repliaction repair"

	def __init__(self,user,password,ip,port,logdir,isdebug):
		Thread.__init__(self)

		self.user = user
		self.password = password
		self.ip = ip
		self.port = port
		self.logdir = logdir
		self.isdebug = isdebug
		self.errorno = 0
		self.dbcursor = self.dbconn(self.ip, self.port).cursor()
		self.lockfile = "/tmp/mysql_repl_repair." + ip + "." + port + ".lck"
		self.logger = MyLogger(self.ip+"."+self.port, self.logdir, self.isdebug)

	def dbconn(self,ip,port):
		"db connection fun, return a db cursor"

		try:
			conn = MySQLdb.connect(user=self.user, passwd=self.password, host=ip,
				port=int(port), db='mysql',cursorclass=MySQLdb.cursors.DictCursor)
			conn.autocommit(True)

			return conn

		except Exception, e:
			raise Exception("can't connect to db, please check user: %s, password: %s, ip: %s, port: %s"\
				%(self.user,self.password,ip,port))

	def get_master_info(self):
		"get master ip and port"
		
		slaveinfo = self.execsql("show slave status")
		
		if slaveinfo is None:
			return 0,0
		else:
			return slaveinfo["Master_Host"],slaveinfo["Master_Port"]

	def execsql(self,sql):
		"execute sql and return result"

		self.logger.debug("start run sql: %s" %(sql))
		self.dbcursor.execute(sql)

		ret = self.dbcursor.fetchone()
		self.logger.debug("sql result: %s" %(str(ret)))

		return ret


	def table_unique_key_info(self,schema_name,table_name):
		"get table unique key"

		self.dbcursor.execute("""select INDEX_NAME, COLUMN_NAME
		from information_schema.STATISTICS
		where table_name = '%s' and NON_UNIQUE = 0 and table_schema ='%s'
		order by INDEX_NAME, SEQ_IN_INDEX""" %(table_name,schema_name))

		res = {}
		rows = self.dbcursor.fetchall()
		for row in rows:
			if row["INDEX_NAME"] not in res:
				res[row["INDEX_NAME"]] = [row["COLUMN_NAME"]]
			else:
				res[row["INDEX_NAME"]].append(row["COLUMN_NAME"])


		return res

	def rowformat_check(self,dbcursor):
		"check binlog format, need be row format"

		dbcursor.execute("select @@binlog_format format")
		ret = dbcursor.fetchone()
		if ret["format"] !="ROW":
			return False
		try:
			dbcursor.execute("select @@binlog_row_image binlog_row_image")
			ret = dbcursor.fetchone()
			if ret["binlog_row_image"] !="FULL":
				return False
		except Exception, e:
			pass

		return True

	def convert_type(self,v):
		if isinstance(v,unicode) or isinstance(v,datetime.datetime)\
			or isinstance(v,datetime.timedelta) or isinstance(v,datetime.date):
			return "'" + str(v) + "'"
		elif isinstance(v,set):
			res = ""
			for i in v:
				res += str(i) + ","
			return '"' + res.rstrip(",") + '"'
		else:
			return v

	def fix_slave_by_sql(self,sql):
		"run sql and restart slave"

		self.logger.info("try to run this sql to resolve repl error, sql: " +sql)
		self.execsql(sql)
		self.execsql("stop slave;")
		self.execsql("start slave")
		time.sleep(0.1)
		slaveinfo = self.execsql("show slave status")

		if slaveinfo["Seconds_Behind_Master"] is not None:
			self.logger.info("slave repl error fixed success!")
			return True
		else:
			self.logger.info("slave repl error fixed failed! go on...")
			return False

	def handle_error(self,rowdata,binlog_pos):
		"handle 1062 & 1032 error with row event result data"

		print rowdata,binlog_pos
		table_schema = rowdata["table_schema"]
		table_name = rowdata["table_name"]
		sql = ""

		if self.errorno == 1062: #duplicate key error
			if rowdata["event_type"] in ("insert","update"): #only insert,update cause 1062 error
				tb_unique_cols = self.table_unique_key_info(table_schema,table_name)

				if tb_unique_cols == {}: #no unique key never cause duplicate key error
					return False

				where_pred = ""
				for uk_name in tb_unique_cols:
					tmp_pred = ""
					for col_name in tb_unique_cols[uk_name]:
						if rowdata["event_type"] == "insert":
							tmp_pred += "and `%s` = %s " %(col_name, self.convert_type(rowdata["data"][col_name]))
						elif rowdata["event_type"] == "update":
							tmp_pred += "and `%s` = %s " %(col_name, self.convert_type(rowdata["data2"][col_name]))
						else:
							raise Exception("something wrong")

					where_pred +=  "or (" + tmp_pred.lstrip("and") + ") "

				sql = "delete from `%s`.`%s` where %s" %(table_schema,table_name, where_pred.lstrip("or"))

		elif self.errorno == 1032: #record not found,update,delete
			if rowdata["event_type"] in ("delete","update"):  # only update & delete cause 1032 error
				tmp_pred = ""
				for col_name in rowdata["data"]:
					if rowdata["data"][col_name] is not None:
						tmp_pred += "`%s` = %s," %(col_name, self.convert_type(rowdata["data"][col_name]))
				sql = "replace into `%s`.`%s` set %s" %(table_schema,table_name,tmp_pred.rstrip(","))

		if sql == "":
			return False
		else:
			if self.stop_position > binlog_pos+4:
				self.execsql(sql)
				return False
			else:
				return self.fix_slave_by_sql(sql)
				


	def run(self):
		"check mysql replication and handle errors"

		global sigint_up

		# add file lock first to sure only 1 process is running on this instance
		if not os.path.exists(self.lockfile):
			os.system("touch %s" %(self.lockfile))

		f = open(self.lockfile, "r")

		try:
			fcntl.flock(f.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
			self.logger.debug("get file lock on %s success" %(self.lockfile))
		except Exception, e:
			msg = "can't get lock for mysql %s, please check script is already running" %(self.port)
			self.logger.error(msg)
			sigint_up = True
			raise Exception(msg)

		while sigint_up==False:
			slaveinfo = self.execsql("show slave status")

			if slaveinfo is None:
				self.logger.info("this instance is not a slave,needn't repair")
				time.sleep(2)
				continue

			if slaveinfo["Seconds_Behind_Master"] is not None:
				self.logger.info("SLAVE IS OK !, SKIP...")

			self.errorno = int(slaveinfo['Last_SQL_Errno'])
			if self.errorno in (1032, 1062):
				#master info
				master_host,master_port = slaveinfo["Master_Host"],int(slaveinfo["Master_Port"])
				master_dbconn = self.dbconn(master_host,int(master_port))
				master_cursor = master_dbconn.cursor()
				
				#master binlog format need be row format
				if not self.rowformat_check(master_cursor):
					sigint_up = True
					raise Exception("unsupport binlog format")
				
				master_log_file = slaveinfo["Relay_Master_Log_File"]
				self.start_position = slaveinfo["Exec_Master_Log_Pos"]

				last_sql_error = slaveinfo["Last_SQL_Error"]
				self.stop_position = int(last_sql_error.split('end_log_pos')[1].split('.')[0])

				self.logger.info("*"*64)
				self.logger.info(" "*10 + "REPL ERROR FOUND !!! STRAT REPAIR ERROR...")
				self.logger.info("*"*64)

				self.logger.info("MASTERLOG FILE : %s " % (master_log_file))
				self.logger.info("START POSITION : %s . STOP POSITION : %s " % (self.start_position, self.stop_position))
				self.logger.info("ERROR MESSAGE : %s" % (last_sql_error))
				self.logger.info("start parse relay log to fix this error...")

				stream = BinLogStreamReader(connection_settings={"host": master_host,"port": master_port,"user": self.user,"passwd": self.password},
							server_id=256256256,
							log_file=master_log_file,
							log_pos=self.start_position,
							resume_stream = True,
							slave_heartbeat=1,
							only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]
						)

				for binlogevent in stream:
					print stream.log_pos
					if stream.log_pos > self.stop_position and stream.log_file == master_log_file:
						break
					else:
						event_info = {"table_name": binlogevent.table, "table_schema": binlogevent.schema}
						
						for row in binlogevent.rows:
							if isinstance(binlogevent, UpdateRowsEvent):
								event_info["event_type"] = "update"
								event_info["data"] = row["before_values"]
								event_info["data2"] = row["after_values"]
							elif isinstance(binlogevent, WriteRowsEvent):
								event_info["event_type"] = "insert"
								event_info["data"] = row["values"]
							elif isinstance(binlogevent, DeleteRowsEvent):
								event_info["event_type"] = "delete"
								event_info["data"] = row["values"]

							res = self.handle_error(event_info, stream.log_pos)
							if res:
								break

			elif self.errorno > 0:
				self.logger.info("this script just can resolve replication error 1062 & 1032")
				self.logger.info("current error is %s, msg: %s" %(slaveinfo['Last_SQL_Errno'], last_sql_error))
				self.logger.info("you should fix it by yourself")

			time.sleep(2)



class MyLogger(object):
	"logger"

	def __init__(self, tag, logdir=None, isdebug=False):
		self.log = logging.getLogger("MYSQLREPLREPAIR" + str(tag))

		self.logdir = logdir
		self.isdebug = isdebug
		self.tag = tag

		self.config_logger()

	def info(self,msg):
		self.log.info("[%s] %s" %(self.tag,msg))

	def debug(self,msg):
		self.log.debug("[%s] %s" %(self.tag,msg))

	def error(self,msg):
		self.log.error("[%s] %s" %(self.tag,msg))

	def warn(self,msg):
		self.log.warn("[%s] %s" %(self.tag,msg))

	def config_logger(self):
		"config logger"
		formatter = logging.Formatter("[%(levelname)s] [%(asctime)s] %(message)s")

		if self.logdir is None:
			loghd = logging.StreamHandler()
		else:
			loghd = logging.FileHandler(self.logdir + "/mysql_repl_repair." + str(self.tag) + ".log",mode='a')

		loghd.setFormatter(formatter)

		if self.isdebug:
			self.log.setLevel(logging.DEBUG)
		else:
			self.log.setLevel(logging.INFO)

		self.log.addHandler(loghd)


def main():
	"main func"

	op = usage()
	print op
	signal.signal(signal.SIGINT, sigint_handler)

	try:
		if op.daemon:
			daemon = MysqlReplRepairDaemon(op)
			daemon.start()
		else:
			run_mysql_repl_repair(op)

	except Exception, e:
		global sigint_up
		sigint_up = True
		print str(e)
		sys.exit()

if __name__ == '__main__' :
	"main func"
	main()
