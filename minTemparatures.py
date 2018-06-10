
# Minimum temparatures reported by each Staion in a year.
# 06092018

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinimumTemparatures")
sc = SparkContext(conf = conf)

def parsedLines(lines):
	fields = lines.split(",")
	stationID = fields[0]
	entryType = fields[2]
	temparature = float(fields[3])*0.1*(9/5)+32.0
	return (stationID,entryType,temparature)


lines = sc.textFile("file:///SparkCourse/1800.csv")
rdd = lines.map(parsedLines)
minTemps = rdd.filter(lambda x : "TMIN" in x[1])
results = minTemps.collect()
for result in results:
	print(result)

stationTemps = minTemps.map(lambda x: (x[0],x[2]))

results = minTemps.collect()
for result in results:
	print(result)
	
stationMinTemps = stationTemps.reduceByKey(lambda x,y : min(x,y))
results = stationMinTemps.collect()

for result in results:
	print(result)
for result in results :
	print(result[0] + '\t' + format(result[1],'0.5f'))






