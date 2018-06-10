
# Problem : To find the average friends for each age Group

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(lines):
	fields = lines.split(",")
	age = int(fields[2])
	numFriends = int(fields[3])
	return (age,numFriends)
 
lines = sc.textFile("file:///SparkCourse/FriendsByAge.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x : (x,1)).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))
averageByAge = totalsByAge.mapValues(lambda x : x[0]/x[1])
results = averageByAge.collect()
for result in results:
	print (result)
 
 
 
 
 