from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	
	def toCSVLine(data):
		return ','.join(str(d) for d in data)
	
	def splitdays(x):
		a = x.split('/')
		return a[1]	

	def splitmonth(x):
		a = x.split('/')
		return a[0]		
    
	lines = sc.textFile(sys.argv[1], 1)

	lines = lines.mapPartitions(lambda x: reader(x))
	
	bronx = lines.filter(lambda x: x[2] == 'BRONX') \
			.map(lambda x: (splitmonth(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()
	
	brooklyn = lines.filter(lambda x: x[2] == 'BROOKLYN') \
			.map(lambda x: (splitmonth(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()	

	manhattan = lines.filter(lambda x: x[2] == 'MANHATTAN') \
			.map(lambda x: (splitmonth(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()

	queens = lines.filter(lambda x: x[2] == 'QUEENS') \
			.map(lambda x: (splitmonth(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()

	staten = lines.filter(lambda x: x[2] == 'STATEN ISLAND') \
			.map(lambda x: (splitmonth(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()			
	
	feb_days_borough_bronx = bronx.map(toCSVLine)
	feb_days_borough_brooklyn = brooklyn.map(toCSVLine)
	feb_days_borough_manhattan = manhattan.map(toCSVLine)
	feb_days_borough_queens = queens.map(toCSVLine)
	feb_days_borough_staten = staten.map(toCSVLine)
	
	feb_days_borough_bronx.saveAsTextFile('feb_days_borough_bronx.csv')
	feb_days_borough_brooklyn.saveAsTextFile('feb_days_borough_brooklyn.csv')
	feb_days_borough_manhattan.saveAsTextFile('feb_days_borough_manhattan.csv')
	feb_days_borough_queens.saveAsTextFile('feb_days_borough_queens.csv')
	feb_days_borough_staten.saveAsTextFile('feb_days_borough_staten.csv')