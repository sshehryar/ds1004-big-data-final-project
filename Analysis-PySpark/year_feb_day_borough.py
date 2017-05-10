from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	
	def toCSVLine(data):
		return ','.join(str(d) for d in data)
	
	def splityear(x):
		a = x.split('/')
		return a[2]	
	
	def splitdays(x):
		a = x.split('/')
		return a[1]	

	def splitmonth(x):
		a = x.split('/')
		return a[0]		
    
	lines = sc.textFile(sys.argv[1], 1)

	lines = lines.mapPartitions(lambda x: reader(x))
	
	bronx = lines.filter(lambda x: x[2] == 'BRONX') \
			.map(lambda x: (splitmonth(x[0]), splityear(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()

	brooklyn = lines.filter(lambda x: x[2] == 'BROOKLYN') \
			.map(lambda x: (splitmonth(x[0]), splityear(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()	

	manhattan = lines.filter(lambda x: x[2] == 'MANHATTAN') \
			.map(lambda x: (splitmonth(x[0]), splityear(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()

	queens = lines.filter(lambda x: x[2] == 'QUEENS') \
			.map(lambda x: (splitmonth(x[0]), splityear(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()

	staten = lines.filter(lambda x: x[2] == 'STATEN ISLAND') \
			.map(lambda x: (splitmonth(x[0]), splityear(x[0]), splitdays(x[0]), x[1], x[2])) \
			.filter(lambda x: x[0] == '02') \
			.map(lambda x: (x,1)).reduceByKey(add).sortByKey()			
	
	year_feb_days_borough_bronx = bronx.map(toCSVLine)
	year_feb_days_borough_brooklyn = brooklyn.map(toCSVLine)
	year_feb_days_borough_manhattan = manhattan.map(toCSVLine)
	year_feb_days_borough_queens = queens.map(toCSVLine)
	year_feb_days_borough_staten = staten.map(toCSVLine)
	
	year_feb_days_borough_bronx.saveAsTextFile('year_feb_days_borough_bronx.csv')
	year_feb_days_borough_brooklyn.saveAsTextFile('year_feb_days_borough_brooklyn.csv')
	year_feb_days_borough_manhattan.saveAsTextFile('year_feb_days_borough_manhattan.csv')
	year_feb_days_borough_queens.saveAsTextFile('year_feb_days_borough_queens.csv')
	year_feb_days_borough_staten.saveAsTextFile('year_feb_days_borough_staten.csv')