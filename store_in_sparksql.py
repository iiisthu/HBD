#import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

# Some columns of the row maybe missing, so complete some parts
def complete_cpu_basic_col(p):
	columns = ["host_sn", "core_n", "freq", "l2_c", "l3_c", "manufacturer", "model", "sn"]
	
	row = {'host': p[1], 'cpu_id': p[2], 'time_stamp': p[3], 'host_sn': 'None', 'core_n': 'None', 'freq': 'None', 'l2_c': 'None', 'l3_c': 'None', 'manufacturer': 'None', 'model': 'None', 'sn': 'None'}		
	for col in range(4, len(p)-1, 2):
		row[p[col]] = p[col+1]
	#Keys and values are listed in an arbitrary order which is non-random, varies across Python implementations, and depends on the dictionarys history of insertions and deletions
	log = "cpu_basic " + p[1] + " " + p[2] + " " + p[3] + " "
	for key in columns:
		log += ' ' + key + ' ' + row[key]	
	
	return log

if __name__ == "__main__":
	sc = SparkContext(appName="StoreDataIntoSparkSQL")
	sqlContext = SQLContext(sc)

	print "Hello!"

	path = "/app/sys/ra/wgs"

	lines = sc.textFile(path+"/small_data_head")
	cpu_basic_logs = lines.filter(lambda l: l.startswith("cpu_basic"))

	#lineLengths = cpu_basic_logs.map(lambda s: len(s))
	#totalLength = lineLengths.reduce(lambda a, b: a + b)
	#print totalLength

	cpu_basic_parts = lines.map(lambda l: l.replace(" ", ":").replace("\t", ":").split(":"))
	#print "The first element of this RDD:", parts.first()
	#logs = parts.map(lambda p: Row(host=p[1], cpu_id=p[2], value=p[3], core_n=p[5], freq=p[7], l2_c=p[9], l3_c=p[11], manufacturer=p[13], sn=p[15]))

	# host cpu_id value(timestamp) core_n freq l2_c l3_c manufacturer sn
	cpu_basic_text = cpu_basic_parts.map(complete_cpu_basic_col)



	#schema_cpu_basic = sqlContext.inferSchema(logs)
	#schema_cpu_basic.registerTempTable("logs")

	#some_cpu = sqlContext.sql("SELECT core_n FROM logs")
	#result = some_cpu.map(lambda p: "host: " + p.core_n)
	#for x in result.collect():
	#	print x

	cpu_basic_text.saveAsTextFile(path + "/output/cpu_basic/small_data_head")

	print "!!!!!!!!!!!!!!!!!!!"

	sc.stop()
