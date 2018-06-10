
# Finding the Max Temparatures by each Station

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemparatures")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/1800.csv")

def parsedLines(lines):
	fields = lines.split(",")
	stationID = fields[0]
	entryType = fields[2]
	temparature = float(fields[3])*0.1*(9/5)+32.0
	return(stationID,entryType,temparature)

rdd = lines.map(parsedLines)
maxTemps = rdd.filter(lambda x : "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x : (x[0],x[2]))
staionMaxTemps = stationTemps.reduceByKey(lambda x,y: max(x,y))
results = staionMaxTemps.collect()
for result in results :
	print(result)
for result in results :
	print(result[0] + '\t' + format(result[1],'0.2f')+'F')
	
	



	

