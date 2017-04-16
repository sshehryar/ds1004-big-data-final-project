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
	
	lines = lines.map(lambda x: (x[1], x[2]))
	
	header = lines.first()
	
	counts = lines.filter(lambda x: x != header) \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	
	total_offenses_borough = counts.map(toCSVLine)
	
	total_offenses_borough.saveAsTextFile('total_offenses_borough.csv')	