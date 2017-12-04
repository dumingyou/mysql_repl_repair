#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author: dumingyou,zhaotianyuan from netease
#this script is used to repair mysql replication errors(1062, 1032)
#1062: HA_ERR_FOUND_DUPP_KEY (duplicate key)
#1032: HA_ERR_KEY_NOT_FOUND (key not found)

import MySQLdb.cursors
import sys,os,fcntl,time,decimal,struct,signal,logging
from optparse import OptionParser
from threading import Thread

sigint_up = False

def usage():
	"Print usage and parse input variables"

	usage = "\n"
	usage += "python " + sys.argv[0] + " [options]\n"
	usage += "\n"
	usage += "this script is used to repair mysql replication errors(1062, 1032)\n"
	usage += "\n"
	usage += "example:\n"
	usage += "python mysql_repl_repair.py -u mysql -p mysql -S /tmp/mysql.sock  -d -v\n"
	usage += "python mysql_repl_repair.py -u mysql -p mysql -S /tmp/mysql3306.sock,"\
			 "/tmp/mysql3307.sock -l /tmp\n"

	parser = OptionParser(usage)

	parser.add_option("-u", "--user", dest="user", action="store",
			help = "username for login mysql")

	parser.add_option("-p", "--password", dest="password", action="store",
			help = "Password to use when connecting to server")

	parser.add_option("-l", "--logdir", dest="logdir", action="store",
			  help = "log will output to screen by default,"\
				  "if run with daemon mode, default logdir is /tmp,"\
				  " logfile is $logdir/mysql_repl_repair.$port.log")

	parser.add_option("-S", "--socket",dest="sockets", action="store",
			help = "mysql sockets for connecting to server, "\
			"you can input multi socket to repair multi mysql instance,"\
			" each socket separate by ','")

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

	if not( options.user and options.password and options.sockets):
		print "please input user password socket"
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

	for socket in op.sockets.split(","):
		myrepair = MysqlReplRepair(op.user, op.password, socket, op.logdir, op.verbose)
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

	def __init__(self,user,password,socket,logdir,isdebug):
		Thread.__init__(self)

		self.user = user
		self.password = password
		self.socket = socket
		self.logdir = logdir
		self.isdebug = isdebug
		self.errorno = 0

		self.dbcursor = self.init_dbconn()
		self.port = self.init_port()

		self.start_position = 0
		self.stop_position = 0

		self.lockfile = "/tmp/mysql_repl_repair" + str(self.port) + ".lck"

		self.logger = MyLogger(self.port, self.logdir, self.isdebug)

		self.logger.info("*"*64)
		self.logger.info(" "*25 + "PROCESS START")
		self.logger.info("*"*64)

	def init_dbconn(self):
		"init db connection, return a db cursor"

		try:
			conn = MySQLdb.connect(user=self.user, passwd=self.password,unix_socket=self.socket,
					db='mysql',cursorclass=MySQLdb.cursors.DictCursor)
			conn.autocommit(True)

			cur = conn.cursor()
			cur.execute("set session sql_log_bin=0")
			return cur

		except Exception, e:
			raise Exception("can't connect to db, please check user: %s, password: %s and socket: %s"\
				%(self.user,self.password,self.socket))

	def init_port(self):
		"get db port"

		self.dbcursor.execute("select @@port port")
		return self.dbcursor.fetchone()["port"]

	def execsql(self,sql):
		"execute sql and return result"

		self.logger.debug("start run sql: %s" %(sql))
		self.dbcursor.execute(sql)

		ret = self.dbcursor.fetchone()
		self.logger.debug("sql result: %s" %(str(ret)))

		return ret

	def get_relay_dir(self):

		ret = self.execsql("select @@relay_log relay_log")
		if ret["relay_log"] is None:
			return self.execsql("select @@datadir datadir")["datadir"]
		elif ret["relay_log"].startswith("/"):
			return ret["relay_log"][:ret["relay_log"].rindex("/")+1]
		else:
			datadir = self.execsql("select @@datadir datadir")["datadir"]
			if ret["relay_log"].startswith("./"):
				return datadir + ret["relay_log"][:ret["relay_log"].rindex("/")+1]
			else:
				return datadir

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

	def rowformat_check(self):
		"check binlog format"

		ret = self.execsql("select @@binlog_format format")
		if ret["format"] !="ROW":
			return False
		try:
			ret = self.execsql("select @@binlog_row_image binlog_row_image")
			if ret["binlog_row_image"] !="FULL":
				return False
		except Exception, e:
			pass

		return True


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

		table_schema = rowdata["table_schema"]
		table_name = rowdata["table_name"]
		sql = ""

		if self.errorno == 1062: #duplicate key error
			if rowdata["event_type"] in (23,30,24,31): #only insert & update cause 1062 error
				tb_unique_cols = self.table_unique_key_info(table_schema,table_name)

				if tb_unique_cols == {}: #no unique key never cause duplicate key error
					return False

				where_pred = ""
				for uk_name in tb_unique_cols:
					tmp_pred = ""
					for col_name in tb_unique_cols[uk_name]:
						if rowdata["event_type"] in (23,30):
							tmp_pred += "and `%s` = %s " %(col_name, rowdata["data"][col_name])
						elif rowdata["event_type"] in (24,31):
							tmp_pred += "and `%s` = %s " %(col_name, rowdata["data2"][col_name])
					where_pred +=  "or (" + tmp_pred.lstrip("and") + ") "

				sql = "delete from `%s`.`%s` where %s" %(table_schema,table_name, where_pred.lstrip("or"))

		elif self.errorno == 1032: #record not found,update,delete
			if rowdata["event_type"] in (24, 25, 31, 32):  # only update & delete cause 1032 error
				tmp_pred = ""
				for col_name in rowdata["data"]:
					if rowdata["data"][col_name] is not None:
						tmp_pred += "`%s` = %s," %(col_name,rowdata["data"][col_name])
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

		# check binlog format
		if not self.rowformat_check():
			sigint_up = True
			raise Exception("unsupport binlog format")

		relaydir = self.get_relay_dir()

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

				binlogfile = relaydir + slaveinfo["Relay_Log_File"]

				self.start_position = slaveinfo["Relay_Log_Pos"]
				last_sql_error = slaveinfo["Last_SQL_Error"]
				relay_log_pos = int(slaveinfo["Relay_Log_Pos"])
				exec_master_log_pos = int(slaveinfo["Exec_Master_Log_Pos"])

				end_log_pos = int(last_sql_error.split('end_log_pos')[1].split('.')[0])

				self.stop_position = relay_log_pos + (end_log_pos - exec_master_log_pos)

				self.logger.info("*"*64)
				self.logger.info(" "*10 + "REPL ERROR FOUND !!! STRAT REPAIR ERROR...")
				self.logger.info("*"*64)

				self.logger.info("RELAYLOG FILE : %s " % (binlogfile))
				self.logger.info("START POSITION : %s . STOP POSITION : %s " % (self.start_position, self.stop_position))
				self.logger.info("ERROR MESSAGE : %s" % (last_sql_error))
				self.logger.info("start parse relay log to fix this error...")

				binlogread = BinlogReader(binlogfile,self.start_position,self.dbcursor,self.logger)

				for event in binlogread:

					if event.rowdata !={}:
						self.logger.debug(str(event))
						res = self.handle_error(event.rowdata, event.start_pos)

						if res:
							break

					if sigint_up or event.start_pos >= self.stop_position:
						break

			elif self.errorno > 0:
				self.logger.info("this script just can resolve replication error 1062 & 1032")
				self.logger.info("current error is %s, msg: %s" %(slaveinfo['Last_SQL_Errno'], last_sql_error))
				self.logger.info("you should fix it by yourself")

			time.sleep(2)


bitCountInByte = [
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
]

# Calculate totol bit counts in a bitmap
def BitCount(bitmap):
	n = 0
	for i in range(0, len(bitmap)):
		bit = bitmap[i]
		if type(bit) is str:
			bit = ord(bit)
		n += bitCountInByte[bit]
	return n

# Get the bit set at offset position in bitmap
def BitGet(bitmap, position):
	bit = bitmap[int(position / 8)]
	if type(bit) is str:
		bit = ord(bit)
	return bit & (1 << (position & 7))

class BinlogReader():
	"""read relay log (binlog) file and return rows
	some function ref python-mysql-replication"""

	def __init__(self,filename,start_pos,dbcursor,logger):
		global sigint_up

		self.filename = filename
		self.start_pos = start_pos
		self.dbcursor = dbcursor
		self.logger = logger

		self.table_column_map = {}
		self.event_remain_len = 0
		self.table_id = None
		self.columns_present_bitmap = None
		self.type_code = 0
		self.rowdata = {}

		try:
			self.stream = open(self.filename, 'rb')
		except Exception, e:
			self.logger.error(str(e))
			sigint_up = True
			raise Exception(str(e))

		self.is_binlogfile()
		self.stream.seek(self.start_pos, 0)

	def __str__(self):
		return "filename: %s,start_pos: %s,rowdata: %s"\
		 %(self.filename,self.start_pos,self.rowdata)

	def read(self, size):
		return self.stream.read(size)

	def is_binlogfile(self):
		"binlog magic number： \xfe\x62\x69\x6e"
		magicnum = self.read(4)
		if magicnum != "\xfe\x62\x69\x6e":
			raise Exception("this is not a binlog file")

	def read_event_header(self):
		"read event head"
		return struct.unpack('<IB3IH', self.read(19))

	def read_uint_by_size(self, size):
		'''Read a little endian integer values based on byte number'''
		if size == 1:
			return struct.unpack('<B', self.read(1))[0]

		elif size == 2:
			return struct.unpack('<H', self.read(2))[0]

		elif size == 3:
			a, b, c = struct.unpack("<BBB", self.read(3))
			return a + (b << 8) + (c << 16)

		elif size == 4:
			return struct.unpack('<I', self.read(4))[0]

		elif size == 5:
			a, b = struct.unpack("<BI", self.read(5))
			return b + (a << 8)

		elif size == 6:
			a, b, c = struct.unpack("<HHH", self.read(6))
			return a + (b << 16) + (c << 32)

		elif size == 7:
			a, b, c = struct.unpack("<BHI", self.read(7))
			return a + (b << 8) + (c << 24)

		elif size == 8:
			return struct.unpack('<Q', self.read(8))[0]

		else:
			raise Exception('Unsupport size.')

	def read_int_be_by_size(self,size):
		'''Read a big endian integer values based on byte number'''

		if size == 1:
			return struct.unpack('>b', self.read(1))[0]

		elif size == 2:
			return struct.unpack('>h', self.read(2))[0]

		elif size == 3:
			a, b, c = struct.unpack('BBB', self.read(3))
			res = (a << 16) | (b << 8) | c
			if res >= 0x800000:
				res -= 0x1000000
			return res

		elif size == 4:
			return struct.unpack('>i', self.read(4))[0]

		elif size == 5:
			a, b = struct.unpack(">IB", self.read(5))
			return b + (a << 8)

		elif size == 8:
			return struct.unpack('>l', self.read(8))[0]

	def read_length_coded_pascal_string(self, size):
		"""Read a string with length coded using pascal style.
	The string start by the size of the string
	"""
		length = self.read_uint_by_size(size)
		return self.read(length)

	def table_column_info(self,schema_name,table_name):
		"get column info from information schema"

		sql = "select * from information_schema.columns where"\
			" table_schema='%s' and table_name='%s' order by ORDINAL_POSITION"
		sql = sql % (schema_name, table_name)
		self.dbcursor.execute(sql)

		return self.dbcursor.fetchall()

	def read_length_coded_binary(self):
		"""Read a 'Length Coded Binary' number from the data buffer.

	Length coded numbers can be anywhere from 1 to 9 bytes depending
	on the value of the first byte
	see https://dev.mysql.com/doc/internals/en/event-content-writing-conventions.html.

	From PyMYSQL source code
	"""
		c = self.read_uint_by_size(1)
		if c == 251: # NULL_COLUMN
			return None,1
		if c < 251: # UNSIGNED_CHAR_COLUMN
			return c,1
		elif c == 252: # UNSIGNED_SHORT_COLUMN
			return struct.unpack('<H',self.read(252)[0:2])[0],253

		elif c == 253: # UNSIGNED_INT24_COLUMN
			n = self.read(253)
			try:
				m = struct.unpack('B', n[0])[0] \
					   + (struct.unpack('B', n[1])[0] << 8) \
					   + (struct.unpack('B', n[2])[0] << 16)
			except TypeError:
				m = n[0] + (n[1] << 8) + (n[2] << 16)

			return m,254

		elif c == 254: # UNSIGNED_INT64_COLUMN
			n = self.read(254)
			try:
				m = struct.unpack('B', n[0])[0] \
					   + (struct.unpack('B', n[1])[0] << 8) \
					   + (struct.unpack('B', n[2])[0] << 16) \
					   + (struct.unpack('B', n[3])[0] << 24)
			except TypeError:
				m = n[0] + (n[1] << 8) + (n[2] << 16) + (n[3] << 24)

			return m,255

	def __read_new_decimal(self, precision, decimals):
		"""Read MySQL's new decimal format introduced in MySQL 5"""

		# This project was a great source of inspiration for
		# understanding this storage format.
		# https://github.com/jeremycole/mysql_binlog

		digits_per_integer = 9
		compressed_bytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4]
		integral = (precision - decimals)
		uncomp_integral = int(integral / digits_per_integer)
		uncomp_fractional = int(decimals / digits_per_integer)
		comp_integral = integral - (uncomp_integral * digits_per_integer)
		comp_fractional = decimals - (uncomp_fractional * digits_per_integer)

		# Support negative
		# The sign is encoded in the high bit of the the byte
		# But this bit can also be used in the value
		value = self.read_uint_by_size(1)
		if value & 0x80 != 0:
			res = ""
			mask = 0
		else:
			mask = -1
			res = "-"

		firstbuf = struct.pack('B', value ^ 0x80)
		firstbuf_len = 1

		size = compressed_bytes[comp_integral]
		if size > 0:
			
			lastbuf = self.read(size-1)
			paddbuf = (4 - size)*'\x00'
			value = struct.unpack('>i', paddbuf + firstbuf+lastbuf)[0] ^ mask
			firstbuf_len = 0

			res += str(value)

		for i in range(0, uncomp_integral):
			if firstbuf_len > 0:
				value = struct.unpack('>i', firstbuf + self.read(4-firstbuf_len))[0] ^ mask
				firstbuf_len = 0
			else:
				value = struct.unpack('>i', self.read(4))[0] ^ mask
			res += '%09d' % value

		res += "."

		for i in range(0, uncomp_fractional):
			if firstbuf_len > 0:
				value = struct.unpack('>i', firstbuf + self.read(4-firstbuf_len))[0] ^ mask
				firstbuf_len = 0
			else:
				value = struct.unpack('>i', self.read(4))[0] ^ mask
			res += '%09d' % value

		size = compressed_bytes[comp_fractional]
		if size > 0:
			if firstbuf_len > 0:
				value = struct.unpack('<i', (4-size)*'\x00' + firstbuf + self.read(size-firstbuf_len))[0] ^ mask
			else:
				value = self.read_int_be_by_size(size) ^ mask
			res += '%0*d' % (int(comp_fractional), value)

		return decimal.Decimal(res)

	def __read_date(self):
		time = self.read_uint_by_size(3)
		if time == 0:  # nasty mysql 0000-00-00 dates
			return None

		year = (time & ((1 << 15) - 1) << 9) >> 9
		month = (time & ((1 << 4) - 1) << 5) >> 5
		day = (time & ((1 << 5) - 1))
		if year == 0 or month == 0 or day == 0:
			return None
		return "%s-%s-%s" %(year,month,day)

	def __read_binary_slice(self, binary, start, size, data_length):
		"""
	Read a part of binary data and extract a number
	binary: the data
	start: From which bit (1 to X)
	size: How many bits should be read
	data_length: data size
	"""
		binary = binary >> data_length - (start + size)
		mask = ((1 << size) - 1)
		return binary & mask

	def __read_fsp(self, fsp):
		read = 0
		if fsp in (1,2):
			read = 1
		elif fsp in (3,4):
			read = 2
		elif fsp in (5,6):
			read = 3
		if read > 0:
			res = '%0' + str(fsp) + 'd'
			microsecond = self.read_int_be_by_size(read)
			if fsp % 2:
				microsecond = int(microsecond / 10)
			return res % microsecond

		return '0'

	def __read_time(self,column):
		"time support microsecond since mysql 5.6"
		# DATETIME_PRECISION field appear in information_columns since mysql 5.6
		if 'DATETIME_PRECISION' in column:
			data = self.read_int_be_by_size(3)

			sign = 1 if self.__read_binary_slice(data, 0, 1, 24) else -1
			if sign == -1:
				# negative integers are stored as 2's compliment
				# hence take 2's compliment again to get the right value.
				data = ~data + 1

			hour=sign * self.__read_binary_slice(data, 2, 10, 24)
			minute=self.__read_binary_slice(data, 12, 6, 24)
			second=self.__read_binary_slice(data, 18, 6, 24)
			microsecond=self.__read_fsp(column["DATETIME_PRECISION"])

			return	str(hour) + ":" + str(minute) + ":"\
					 + str(second)  + '.' + microsecond
		else:
			time = self.read_uint_by_size(3)
			hour=int(time / 10000)
			minute=int((time % 10000) / 100)
			second=int(time % 100)
			return str(hour) + ":" + str(minute) + ":" + str(second)

	def __read_datetime(self, column):
		"datetime support microsecond since mysql 5.6"

		# DATETIME_PRECISION field appear in information_columns since mysql 5.6
		if 'DATETIME_PRECISION' in column:
			data = self.read_int_be_by_size(5)
			year_month = self.__read_binary_slice(data, 1, 17, 40)
			try:
				year=int(year_month / 13)
				month=year_month % 13
				day=self.__read_binary_slice(data, 18, 5, 40)
				hour=self.__read_binary_slice(data, 23, 5, 40)
				minute=self.__read_binary_slice(data, 28, 6, 40)
				second=self.__read_binary_slice(data, 34, 6, 40)

				date_precision = column["DATETIME_PRECISION"]
				microsecond = self.__read_fsp(date_precision)

				return str(year) + "-" + str(month) + "-" + str(day)\
					+ " " + str(hour) + ":" + str(minute)\
					+ ":" + str(second) + "." + microsecond
			except ValueError:
				return None

		#before mysql 5.6
		else:
			value = self.read_uint_by_size(8)
			if value == 0:  # nasty mysql 0000-00-00 dates
				return None

			return str(value)[:8] + ' ' + str(value)[8:]

	def __read_timestamp(self,column):
		"timestamp support microsecond since mysql 5.6"
		if 'DATETIME_PRECISION' in column:
			ts = self.read_int_be_by_size(4)
			microsecond = self.__read_fsp(column["DATETIME_PRECISION"])
			return str(ts)+'.' + microsecond
		else:
			return self.read_uint_by_size(4)

	def __read_bit(self, bytes,length):
		"""Read MySQL BIT type"""
		resp = ""
		for byte in range(0, bytes):
			current_byte = ""
			data = self.read_uint_by_size(1)
			if byte == 0:
				if bytes == 1:
					end = length
				else:
					end = length % 8
					if end == 0:
						end = 8
			else:
				end = 8
			for bit in range(0, end):
				if data & (1 << bit):
					current_byte += "1"
				else:
					current_byte += "0"
			resp += current_byte[::-1]
		return resp

	def __is_null(self, null_bitmap, position):
		bit = null_bitmap[int(position / 8)]
		if type(bit) is str:
			bit = ord(bit)
		return bit & (1 << (position % 8))


	def _read_column_data(self, cols_bitmap, column_info):
		"""Use for WRITE, UPDATE and DELETE events.
		Return an array of column data
		"""
		values = {}

		# null bitmap length = (bits set in 'columns-present-bitmap'+7)/8
		# See http://dev.mysql.com/doc/internals/en/rows-event.html
		null_bitmap = self.read((BitCount(cols_bitmap) + 7) / 8)

		nullBitmapIndex = 0
		nb_columns = len(column_info)
		for i in range(0, nb_columns):
			column = column_info[i]
			name = column['COLUMN_NAME']

			if "unsigned" in column["COLUMN_TYPE"]:
				unsigned = True
			else:
				unsigned = False

			if BitGet(cols_bitmap, i) == 0:
				values[name] = None
				continue

			col_start_pos = self.stream.tell()

			if self.__is_null(null_bitmap, nullBitmapIndex):
				values[name] = None
			elif column["DATA_TYPE"] == "tinyint": # tinyint & boolean
				if unsigned:
					values[name] = struct.unpack("<B", self.read(1))[0]
				else:
					values[name] = struct.unpack("<b", self.read(1))[0]
			elif column["DATA_TYPE"] == "smallint": #smallint:
				if unsigned:
					values[name] = struct.unpack("<H", self.read(2))[0]
				else:
					values[name] = struct.unpack("<h", self.read(2))[0]
			elif column["DATA_TYPE"] == "int": #int:
				if unsigned:
					values[name] = struct.unpack("<I", self.read(4))[0]
				else:
					values[name] = struct.unpack("<i", self.read(4))[0]
			elif column["DATA_TYPE"] == "mediumint": #mediumint:
				if unsigned:
					values[name] = self.read_uint_by_size(3)
				else:
					a, b, c = struct.unpack("BBB", self.read(3))
					res = a | (b << 8) | (c << 16)
					if res >= 0x800000:
						res -= 0x1000000
					
					values[name] = res

			elif column["DATA_TYPE"] == "float": #float:
				values[name] = struct.unpack("<f", self.read(4))[0]

			elif column["DATA_TYPE"] == "double":
				values[name] = struct.unpack("<d", self.read(8))[0]

			elif "char" in column["DATA_TYPE"] or "text" in column["DATA_TYPE"] \
				or "blob" in 	column["DATA_TYPE"] or "binary" in column["DATA_TYPE"]:
				if column["CHARACTER_OCTET_LENGTH"] > 255:
					value = self.read_length_coded_pascal_string(2)
				else:
					value = self.read_length_coded_pascal_string(1)
				values[name] = "x'"+ value.encode('hex') + "'"

			elif column["DATA_TYPE"] == "decimal":
				PRECISION = column["NUMERIC_PRECISION"]
				SCALE = column["NUMERIC_SCALE"]
				values[name] = self.__read_new_decimal(PRECISION,SCALE)

			elif column["DATA_TYPE"] == "datetime":
				values[name] = "'" + self.__read_datetime(column) + "'"

			elif column["DATA_TYPE"] == "time":
				values[name] = "'" + self.__read_time(column) + "'"

			elif column["DATA_TYPE"] == "date":
				values[name] = "'" + self.__read_date() + "'"

			elif column["DATA_TYPE"] == "timestamp":
				values[name] = "from_unixtime("+str(self.__read_timestamp(column))+")"

			elif column["DATA_TYPE"] == "bigint":
				if unsigned:
					values[name] = self.read_uint_by_size(8)
				else:
					values[name] = struct.unpack('<q', self.read(8))[0]

			elif column["DATA_TYPE"] == "year":
				values[name] = "'" + str(self.read_uint_by_size(1) + 1900) + "'"

			elif column["DATA_TYPE"] == "enum":
				size = len(column["COLUMN_TYPE"].split("','")) + 1
				if size < 256:
					values[name] = self.read_uint_by_size(1)
				else:
					values[name] = self.read_uint_by_size(2)

			elif column["DATA_TYPE"] == "set":
				# We read set columns as a bitmap telling us which options
				# are enabled
				size = len(column["COLUMN_TYPE"].split("','")) + 1
				if size <= 8:
					values[name] = self.read_uint_by_size(1)
				elif size <= 16:
					values[name] = self.read_uint_by_size(2)
				elif size <= 24:
					values[name] = self.read_uint_by_size(3)
				elif size <= 64:
					values[name] = self.read_uint_by_size(8)
				else:
					raise Exception("error found while parse set type data")

			elif column["DATA_TYPE"] == "bit":
				length = int(column["COLUMN_TYPE"].split('(')[1][:-1])
				size = (length+7)/8
				values[name] = "b'" + self.__read_bit(size,length) + "'"

			#todo support GEOMETRY & JSON
			# elif column.type == FIELD_TYPE.GEOMETRY:
			# 	values[name] = self.packet.read_length_coded_pascal_string(1)
			# elif column.type == FIELD_TYPE.JSON:
			# 	values[name] = self.packet.read_binary_json(column.length_size)
			else:
				raise Exception("Unknown MySQL column type: %d" %(column["DATA_TYPE"]))

			nullBitmapIndex += 1
			col_end_pos = self.stream.tell()
			self.logger.debug("read column for %s.%s, column: %s, data type: %s, read bytes %s,"\
				" column value: %s" %(column['TABLE_SCHEMA'],column['TABLE_NAME'],\
				name,column["DATA_TYPE"], col_end_pos - col_start_pos, values[name] ))

		return values

	def __next__(self):
		"for support python3.0"

		return self.next()

	def _read_data(self):
		"direct read data"

		self.rowdata = {}
		if self.event_remain_len > 0:
			if self.event_remain_len != 4: #4 bytes checksum, at the end of event if checksum enable

				self.rowdata["data"] = {}
				self.rowdata["data2"] = {}

				braw_pos = self.stream.tell()
				self.rowdata["data"]= self._read_column_data(self.columns_present_bitmap,self.table_column_map[self.table_id])
				self.rowdata["table_schema"] = self.table_column_map[self.table_id][0]["TABLE_SCHEMA"]
				self.rowdata["table_name"] = self.table_column_map[self.table_id][0]["TABLE_NAME"]
				self.rowdata["event_type"] = self.type_code


				if self.type_code in (24,31):
					#  new record of update row event
					self.rowdata["data2"]= self._read_column_data(self.columns_present_bitmap,self.table_column_map[self.table_id])

				araw_pos = self.stream.tell()

				self.event_remain_len = self.event_remain_len - (araw_pos - braw_pos)
			else:
				self.stream.seek(4, 1)
				self.event_remain_len = 0


	def next(self):
		"parse row format binlog iteratively"

		if self.event_remain_len > 0:
			self._read_data()

		else:

			timestamp, self.type_code, server_id, event_length, next_position, flags = self.read_event_header()

			if event_length == 0:
				raise Exception("event length is 0, something error")

			# TABLE_MAP_EVENT
			if self.type_code == 19:
				self.table_id = self.read_uint_by_size(6) #6 bytes. The table ID
				self.stream.seek(2,1)#2 bytes. Reserved for future use
				schema_length = self.read_uint_by_size(1)
				schema_name = self.read(schema_length).decode()#schema name
				self.stream.seek(1, 1)#schema name terminated with null
				table_length = self.read_uint_by_size(1)
				table_name = self.read(table_length).decode()#table name
				self.table_column_map[self.table_id] = self.table_column_info(schema_name,table_name)
				self.stream.seek(event_length - (19 + 6 + 2 + 1 + schema_length + 1 + 1 + table_length), 1)

			# before 5.6 WRITE/UPDATE/DELETE_ROWS_EVENT type code is 23 24 25
			# start 5.6 WRITE/UPDATE/DELETE_ROWS_EVENT type code change to 30 31 32
			elif self.type_code in (23,24,25,30,31,32):
				# 6 bytes. The table ID
				self.table_id = self.read_uint_by_size(6) #6 bytes. The table ID
				if self.table_id not in self.table_column_map:
					self.logger.error("ERROR: METADATA WRONG, TABLE_ID IS NOT RIGHT , EXIT...")

				self.stream.seek(2, 1)  # 2 bytes. Reserved for future use

				if self.type_code >= 30:
					extra_len = 2
				else:
					extra_len = 0

				self.stream.seek(extra_len, 1) #extra_data_length

				number_of_columns,col_count_len = self.read_length_coded_binary()

				columns_in_use_len = (number_of_columns + 7) / 8
				self.columns_present_bitmap = self.read(columns_in_use_len)

				if self.type_code in (24,31):
					# update row event has two column bit map
					columns_exists_bitmap_size = columns_in_use_len * 2
					columns_present_bitmap2 = self.read(columns_in_use_len)
				else:
					columns_exists_bitmap_size = columns_in_use_len

				extra = (19+6+2+col_count_len+columns_exists_bitmap_size+extra_len)
				self.event_remain_len = event_length - extra

				self._read_data()

			else:
				# skip parse other event
				self.stream.seek(event_length - 19, 1)

		self.start_pos = self.stream.tell()
		return self


	def __iter__(self):
		return self

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
