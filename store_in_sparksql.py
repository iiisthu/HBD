import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row


def get_log(log_name, parts, row, columns, start_index = 4):
	for col in range(start_index, len(parts)-1, 2):
		row[parts[col]] = parts[col+1]
    #Keys and values are listed in an arbitrary order which is non-random, varies across Python implementations, and depends on the dictionarys history of insertions and deletions
	#log = log_name + " " + parts[1] + " " + parts[2] + " " + parts[3] + " "
	log = log_name
	for key in columns:
		#log += ' ' + key + ' ' + row.get(key, "None") 
		log += ' ' + row.get(key, "None")

	return log


# Some columns of the row maybe missing, so complete some parts
def complete_cpu_basic(line):
	columns = ["host", "cpu_id", "time_stamp", "host_sn", "core_n", "freq", "l2_c", "l3_c", "manufacturer", "model", "sn"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'cpu_id': p[2], 'time_stamp': p[3]}
	log = get_log("cpu_basic", p, row, columns, 4)
	return log

def complete_cpu_failure(line):
	columns = ["host_sn", "reason", "record_id", "result", "source"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'cpu_id': p[2], 'time_stamp': p[3]}
	log = get_log("cpu_failure", p, row, columns, 4)
	return log

def complete_cpu_performance(line):
	columns = ["host", "cpu_id", "time_stamp", "host_sn", "cur_freq", "ht", "norm", "pstate", "record_id", "turbo"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'cpu_id': p[2], 'time_stamp': p[3]}
	log = get_log("cpu_performance", p, row, columns, 4)
	return log	

def complete_ctrl_basic(line):
	columns = ["host_sn", "firmware", "manufacturer", "model", "sn"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'ctrl_id': p[2], 'time_stamp': p[3]}
	log = get_log("ctrl_basic", p, row, columns, 4)
	return log

def complete_ctrl_failure(line):
	columns = ["host_sn", "reason", "record_id", "result", "source"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'ctrl_id': p[2], 'time_stamp': p[3]}
	log = get_log("ctrl_failure", p, row, columns, 4)
	return log

def complete_ctrl_status(line):
	columns = ["host_sn", "bcp", "epc", "record_id", "temp"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'ctrl_id': p[2], 'time_stamp': p[3]}
	log = get_log("ctrl_status", p, row, columns, 4)
	return log

def complete_custom_has_config(line):
	columns = ["monitor", "power", "predict", "prepair", "service_down", "service_up", "soa", "version"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'time_stamp': p[3]}
	log = get_log("custom_has_config", p, row, columns, 4)
	return log

def complete_dimm_basic(line):
	columns = ["host_sn", "capacity", "freq", "logicalslot", "manufacturer", "model", "slot", "sn", "volt"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'dimm_id': p[3]}
	log = get_log("dimm_basic", p, row, columns, 4)
	return log

def complete_dimm_failure(line):
	columns = ["host_sn", "reason", "record_id", "result", "source"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'dimm_id': p[2], 'time_stamp': p[3]}
	log = get_log("dimm_failure", p, row, columns, 4)
	return log

def complete_dimm_performance(line):
	columns = ["host_sn", "cur_freq", "numa", "record_id"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'dimm_id': p[2], 'time_stamp': p[3]}
	log = get_log("dimm_performance", p, row, columns, 4)
	return log

def complete_hdd_basic(line):
	columns = ["host_sn", "capacity", "did", "firmware", "interface", "lss", "manufacturer", "media", "model", "pss", "rpm", "sn"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'pd_id': p[2], 'time_stamp': p[3]}
	log = get_log("hdd_basic", p, row, columns, 4)
	return log

def complete_hdd_power(line):
	columns = ["host_sn", "epc", "mode", "record_id"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'device_id': p[2], 'time_stamp': p[3]}
	log = get_log("hdd_power", p, row, columns, 4)
	return log

def complete_hdd_smart(line):
	#TODO
	#????????????????????????????????
	return ""

def complete_hdd_status(line):
	columns = ["host_sn", "capacity", "did", "firmware", "interface", "lss", "manufacturer", "media", "model", "pss", "rpm", "sn"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'device_id': p[2], 'time_stamp': p[3]}
	log = get_log("hdd_status", p, row, columns, 4)
	return log

def complete_nic_basic(line):
	columns = ["host_sn", "bandwidth", "manufacturer", "sn", "version"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'eth_id': p[2], 'time_stamp': p[3]}
	log = get_log("nic_basic", p, row, columns, 4)
	return log

def complete_nic_failure(line):
	columns = ["host_sn", "reason", "record_id", "result", "source"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'eth_id': p[2], 'time_stamp': p[3]}
	log = get_log("nic_failure", p, row, columns, 4)
	return log

def complete_nic_status(line):
	columns = ["host_sn", ]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'eth_id': p[2], 'time_stamp': p[3]}
	log = get_log("nic_status", p, row, columns, 4)
	return log

def complete_psu_basic(line):
	columns = ["host_sn", "id", "model"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'psu_id': p[2], 'time_stamp': p[3]}
	log = get_log("psu_basic", p, row, columns, 4)
	return log

def complete_server_basic(line):
	columns = ["host_sn", "dimm_mn", "hotplug", "manufacturer", "model", "online_time", "sn"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'time_stamp': p[3]}
	log = get_log("server_basic", p, row, columns, 4)
	return log

def complete_server_health(line):
	columns = ["host_sn", "code"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'time_stamp': p[3]}
	log = get_log("server_health", p, row, columns, 4)
	return log

#TODO
#??????????????????????????????????????? what is the host
def complete_server_overall_status(line):
	columns = ["host_sn", "avg_power", "avt_temp", "bios_v", "bmc_v", "capping_power", "capping_temp", "cpu_n", "epc", "hdoctor_v", "mb_v", "peak_power", "peak_temp", "procuct", "rack_info", "record_id"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'time_stamp': p[3]}
	log = get_log("server_overall_status", p, row, columns, 4)
	return log

def complete_server_rt_status(line):
	columns = ["host", "time_stamp", "host_sn", "cpupower", "dimmpower", "in_temp", "out_temp", "pch_temp", "power", "power_ratio", "record_id"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'time_stamp': p[2]}
	log = get_log("server_rt_status", p, row, columns, 3)
	return log

def complete_vd_failure(line):
	columns = ["host_sn", "reason", "record_id", "result", "source"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'vd_id': p[2], 'time_stamp': p[3]}
	log = get_log("vd_failure", p, row, columns, 4)
	return log

def complete_vd_status(line):
	columns = ["host_sn", "label", "level", "pd_n", "pid", "rcp", "record_id", "rw_r"]

	p = line.replace(":", " ").split()
	row = {'host': p[1], 'vd_id': p[2], 'time_stamp': p[3]}
	log = get_log("vd_status", p, row, columns, 4)
	return log




##===================================================================
if __name__ == "__main__":
	sc = SparkContext(appName="StoreDataIntoSparkSQL")
	sqlContext = SQLContext(sc)

	print "Hello!"

	path = "/app/sys/ra/wgs"

	lines = sc.textFile(path+"/20141201_0100")
	#cpu_basic_logs = lines.filter(lambda l: l.startswith("cpu_basic"))
	#cpu_failure_logs = lines.filter(lambda l: l.startswith("cpu_failure"))
	server_rt_logs = lines.filter(lambda l: l.startswith("cpu_performance"))

	# host cpu_id value(timestamp) core_n freq l2_c l3_c manufacturer sn
	#cpu_basic_text = cpu_basic_logs.map(complete_cpu_basic)
	#cpu_basic_text = cpu_failure_logs.map(complete_cpu_failure)
	
	server_rt_text = server_rt_logs.map(complete_cpu_performance) 

	#schema_cpu_basic = sqlContext.inferSchema(logs)
	#schema_cpu_basic.registerTempTable("logs")

	#some_cpu = sqlContext.sql("SELECT core_n FROM logs")
	#result = some_cpu.map(lambda p: "host: " + p.core_n)
	#for x in result.collect():
	#   print x

	#server_rt_text.saveAsTextFile(path + "/output/server_rt_status/20141201_0100")	
	server_rt_text.saveAsTextFile(path + "/output/cpu_performance/20141201_0100")
	print "!!!!!!!!!!!!!!!!!!!ending"

	sc.stop()



