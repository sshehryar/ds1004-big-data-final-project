from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	
	def toCSVLine(data):
		return ','.join(str(d) for d in data)
    
	lines = sc.textFile(sys.argv[1], 1)

	lines = lines.mapPartitions(lambda x: reader(x))
	
	lines = lines.map(lambda x: (x[1]))
	
	header = lines.first()
	
	counts = lines.filter(lambda x: x != header) \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	
	total_offenses = counts.map(toCSVLine)
	
	total_offenses.saveAsTextFile('total_offenses.csv')	