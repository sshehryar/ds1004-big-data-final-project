from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	
	def toCSVLine(data):
		return ','.join(str(d) for d in data)
		
	def splitmonth(x):
		a = x.split('/')
		return a[0]		
    
	lines = sc.textFile(sys.argv[1], 1)

	lines = lines.mapPartitions(lambda x: reader(x))
	
	bronx = lines.filter(lambda x: x[2] == 'BRONX') \
			.map(lambda x: (splitmonth(x[0]), x[1], x[2])) \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	
	brooklyn = lines.filter(lambda x: x[2] == 'BROOKLYN') \
			.map(lambda x: (splitmonth(x[0]), x[1], x[2])) \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()	

	manhattan = lines.filter(lambda x: x[2] == 'MANHATTAN') \
			.map(lambda x: (splitmonth(x[0]), x[1], x[2])) \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()

	queens = lines.filter(lambda x: x[2] == 'QUEENS') \
			.map(lambda x: (splitmonth(x[0]), x[1], x[2])) \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()

	staten = lines.filter(lambda x: x[2] == 'STATEN ISLAND') \
			.map(lambda x: (splitmonth(x[0]), x[1], x[2])) \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()			
	
	total_offenses_month_bronx = bronx.map(toCSVLine)
	total_offenses_month_brooklyn = brooklyn.map(toCSVLine)
	total_offenses_month_manhattan = manhattan.map(toCSVLine)
	total_offenses_month_queens = queens.map(toCSVLine)
	total_offenses_month_staten = staten.map(toCSVLine)
	
	total_offenses_month_bronx.saveAsTextFile('total_offenses_month_bronx.csv')
	total_offenses_month_brooklyn.saveAsTextFile('total_offenses_month_brooklyn.csv')
	total_offenses_month_manhattan.saveAsTextFile('total_offenses_month_manhattan.csv')
	total_offenses_month_queens.saveAsTextFile('total_offenses_month_queens.csv')
	total_offenses_month_staten.saveAsTextFile('total_offenses_month_staten.csv')