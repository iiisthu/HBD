#import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

sc = SparkContext(appName="people")
sqlContext = SQLContext(sc)



path = "/app/sys/ra/wgs"

# Load a text file and convert each line to a dictionary.
lines = sc.textFile(path+"/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the SchemaRDD as a table.
schemaPeople = sqlContext.inferSchema(people)
schemaPeople.registerTempTable("people")

# SQL can be run over SchemaRDDs that have been registered as a table.
#teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
#teenNames = teenagers.map(lambda p: "Name: " + p.name)
#for teenName in teenNames.collect():
#  print teenName

parquetFile = schemaPeople.saveAsParquetFile(path+"/output")







sc.stop()
