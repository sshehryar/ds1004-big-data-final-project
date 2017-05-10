from __future__ import print_function
from csv import reader
import sys
from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))
	
	newyork = lines.filter(lambda x: x[1] == 'NY CITY CENTRAL PARK NY US').map(lambda x: (x[5][:6], x[14], x[15], x[12]))
	
	newyorkmin = newyork.filter(lambda x: x[2] != '-9999').map(lambda x: (x[0], int(x[2]))).combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1])) \
							.map(lambda (label, (value_sum, count)): (label, ("{0:.2f}".format(value_sum / count))))
							
	newyorkmax = newyork.filter(lambda x: x[1] != '-9999').map(lambda x: (x[0], int(x[1]))).combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1])) \
							.map(lambda (label, (value_sum, count)): (label, ("{0:.2f}".format(value_sum / count))))	
	
	newyorksnow = newyork.filter(lambda x: x[3] != '-9999').map(lambda x: (x[0], float(x[3]))).combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1])) \
							.map(lambda (label, (value_sum, count)): (label, ("{0:.2f}".format(value_sum / count))))	
	
	newyorkresult = newyorkmax.join(newyorkmin).join(newyorksnow).sortByKey()
	
	newyorkresult.map(lambda x: "{0},{1},{2},{3}".format(x[0],x[1][0][0],x[1][0][1], x[1][1])).saveAsTextFile("newyork_weather.csv")
	
	sc.stop()