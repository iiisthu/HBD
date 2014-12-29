import sys
from pyspark import SparkContext
from pyspark.sql import *

field_cpu_failure = "host cpu_id time_stamp host_sn reason record_id result source"

def construct_schemaRDD(parts, field_names, field_types):
	fields = field_names.split()
	length = len(fields) # the length of true values
	rowRDD = parts.map(lambda p: tuple([p[i+1] for i in range(length)]))
	#print len(fields), len(field_types), length
	columns = [StructField(fields[k], field_types[k], True) for k in range(length)]
	schema = StructType(columns)
	schemaRDD = sqlContext.applySchema(rowRDD, schema)
	
	return schemaRDD

def get_table_server_rt_status(lines):
	def convert_types(line):
		parts = line.split(' ')
		parts[2] = long(parts[2]) if parts[2] != 'None' else None
		for k in range(4, 11):
			parts[k] = long(parts[k]) if parts[k] != 'None' else None
		return parts

	field_names = "host timestamp host_sn cpupower dimmpower in_temp out_temp pch_temp power power_ratio record_id"
	field_types = [StringType(), LongType(), StringType(), IntegerType(), IntegerType(), IntegerType(), IntegerType(), IntegerType(), IntegerType(), IntegerType(), StringType()]	
	parts = lines.map(convert_types)	
	schemaRDD = construct_schemaRDD(parts, field_names, field_types)
	table = schemaRDD.registerTempTable("server_rt_status")
	return table


def get_table_cpu_performance(lines):
    def convert_types(line):
		parts = line.split(' ')
		parts[3] = long(parts[3]) if parts[3] != 'None' else None
		parts[5] = int(parts[5]) if parts[5] != 'None' else None
		parts[7] = float(parts[7]) if parts[7] != 'None' else None
		return parts

    field_names = "host cpu_id timestamp host_sn cur_freq ht norm pstate record_id turbo"
    field_types = [StringType(), StringType(), LongType(), StringType(), IntegerType(), StringType(), FloatType(), StringType(), StringType(), StringType()]
    parts = lines.map(convert_types)
    schemaRDD = construct_schemaRDD(parts, field_names, field_types)
    table = schemaRDD.registerTempTable("cpu_performance")
    return table




###=====================================================================================================

if __name__ == "__main__":
	sc = SparkContext(appName="ReadDataIntoSparkSQL")
	sqlContext = SQLContext(sc)

	print "Hello!"

	path = "/app/sys/ra/wgs"

#	lines1 = sc.textFile(path+"/output/cpu_performance/20141201_0100/part-000[0-9]*")
#	table_cpu_performance = get_table_cpu_performance(lines1)
#	results = sqlContext.sql("SELECT host,timestamp,cur_freq,turbo FROM cpu_performance WHERE host = '10.86.148.11'")
#	results.map(lambda p: "host:" + p.host + " timestamp:" + p.timestamp + " cur_freq:" + p.cur_freq + " turbo:" + p.turbo)
#	for record in results.collect():
#		print record

#	print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", results


	lines2 = sc.textFile(path+"/output/server_rt_status/20141201_0100/part-000[0-9]*")
	table_server_rt_status = get_table_server_rt_status(lines2)
	results = sqlContext.sql("SELECT host,timestamp,cpupower,dimmpower FROM server_rt_status WHERE host = '10.86.148.11' AND timestamp/100 >= 14173669 AND timestamp/100 <= 14173675")
	records = results.map(lambda p: "host:" + p.host + " timestamp:" + str(p.timestamp) + " cpupower:" + str(p.cpupower) + " dimmpower:" + str(p.dimmpower))
	for record in records.collect():
		print record
#	results = sqlContext.sql("SELECT cpu_performance.host,cpu_performance.timestamp,cpu_performance.cpu_id,cpu_performance.turbo,server_rt_status.cpupower FROM cpu_performance,server_rt_status WHERE cpu_performance.host = '10.86.148.11' AND server_rt_status.host = '10.86.148.11' AND server_rt_status.timestamp - cpu_performance.timestamp < 60 AND server_rt_status.timestamp - cpu_performance.timestamp > -60")	
	#print results.count()
#	print "+++++++++++++++++++++++++++++++++"
#	records = results.map(lambda p: "host:" + p.host + " cpu_id:" + p.cpu_id + " timestamp:" + str(p.timestamp) + " turbo:" + p.turbo + " cpupower:" + str(p.cpupower))
#	for record in records.collect():
#	    print record	
	

	print "###############################"

	sc.stop()
