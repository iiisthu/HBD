#import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row


def merge_same(a, b):
	result = []
	for x in a:
		result.append[x]
	for y in b:
		pass
	return result

if __name__ == "__main__":
	sc = SparkContext(appName="StoreDataIntoSparkSQL")
	sqlContext = SQLContext(sc)

	print "Hello!"

	path = "/app/sys/ra/wgs"

	lines = sc.textFile(path+"/output/cpu_basic/miss/part-000[0-9]*")
	#missing_logs = lines.filter(lambda l: l.startswith("cpu_basic"))
	#miss_info = lines.map(lambda l: [l])
	totalLength = lines.countByValue()

	print totalLength

	print "!!!!!!!!!!!!!!!!!!!"

	sc.stop()
