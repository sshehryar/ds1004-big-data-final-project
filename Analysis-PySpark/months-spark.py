from __future__ import print_function

import sys
from csv import reader
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))
	
	counts = lines.map(lambda line: (line[0][-4:], line[0][:2])).map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	counts = counts.map(lambda x: "{0},{1},{2}".format(x[0][0],x[0][1],x[1])).saveAsTextFile("months-crimes.csv")
	
	bronx = lines.filter(lambda x: x[2] == 'BRONX').map(lambda line: (line[0][-4:], line[0][:2])).map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	bronx = bronx.map(lambda x: "{0},{1},{2}".format(x[0][0],x[0][1],x[1])).saveAsTextFile("months-crimes-bronx.csv")
	
	brooklyn = lines.filter(lambda x: x[2] == 'BROOKLYN').map(lambda line: (line[0][-4:], line[0][:2])).map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	brooklyn = brooklyn.map(lambda x: "{0},{1},{2}".format(x[0][0],x[0][1],x[1])).saveAsTextFile("months-crimes-brooklyn.csv")
	
	manhattan = lines.filter(lambda x: x[2] == 'MANHATTAN').map(lambda line: (line[0][-4:], line[0][:2])).map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	manhattan = manhattan.map(lambda x: "{0},{1},{2}".format(x[0][0],x[0][1],x[1])).saveAsTextFile("months-crimes-manhattan.csv")
	
	queens = lines.filter(lambda x: x[2] == 'QUEENS').map(lambda line: (line[0][-4:], line[0][:2])).map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	queens = queens.map(lambda x: "{0},{1},{2}".format(x[0][0],x[0][1],x[1])).saveAsTextFile("months-crimes-queens.csv")
	
	staten = lines.filter(lambda x: x[2] == 'STATEN').map(lambda line: (line[0][-4:], line[0][:2])).map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	staten = staten.map(lambda x: "{0},{1},{2}".format(x[0][0],x[0][1],x[1])).saveAsTextFile("months-crimes-staten.csv")
	
	sc.stop()