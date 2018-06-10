
# Average Movie Rating on 100k Movie IDs

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf =conf)



line = sc.textFile("file:///SparkCourse/ml-100k/u.data")
ratings = line.map(lambda x:x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(key,value)
