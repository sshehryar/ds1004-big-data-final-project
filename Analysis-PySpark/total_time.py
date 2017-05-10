from __future__ import print_function

import sys
from csv import reader
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))
	
	def splithour(x):
		a = x.split(':')
		return a[0]

	bronx = lines.filter(lambda x: x[2] == 'BRONX').map(lambda x: (splithour(x[0]), x[1]))
	bronxlarceny = bronx.filter(lambda x: x[1] == '341').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("bronx-larceny.csv")
	bronxharrasment = bronx.filter(lambda x: x[1] == '578').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("bronx-harrasment.csv")
	bronxassault = bronx.filter(lambda x: x[1] == '344').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("bronx-assault.csv")				
					
	brooklyn = lines.filter(lambda x: x[2] == 'BROOKLYN').map(lambda x: (splithour(x[0]), x[1]))
	brooklynlarceny = brooklyn.filter(lambda x: x[1] == '341').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("brooklyn-larceny.csv")
	brooklynharrasment = brooklyn.filter(lambda x: x[1] == '578').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("brooklyn-harrasment.csv")
	brooklynassault = brooklyn.filter(lambda x: x[1] == '344').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("brooklyn-assault.csv")				
	
	manhattan = lines.filter(lambda x: x[2] == 'MANHATTAN').map(lambda x: (splithour(x[0]), x[1]))
	manhattanlarceny = manhattan.filter(lambda x: x[1] == '341').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("manhattan-larceny.csv")
	manhattanharrasment = manhattan.filter(lambda x: x[1] == '578').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("manhattan-harrasment.csv")
	manhattangrandlarceny = manhattan.filter(lambda x: x[1] == '109').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("manhattan-grand-larceny.csv")				
	
	queens = lines.filter(lambda x: x[2] == 'QUEENS').map(lambda x: (splithour(x[0]), x[1]))
	queenslarceny = queens.filter(lambda x: x[1] == '341').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("queens-larceny.csv")
	queensharrasment = queens.filter(lambda x: x[1] == '578').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("queens-harrasment.csv")
	queensassault = queens.filter(lambda x: x[1] == '344').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("queens-assault.csv")				
	
	staten = lines.filter(lambda x: x[2] == 'STATEN ISLAND').map(lambda x: (splithour(x[0]), x[1]))
	statenlarceny = staten.filter(lambda x: x[1] == '341').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("staten-larceny.csv")
	statenharrasment = staten.filter(lambda x: x[1] == '578').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("staten-harrasment.csv")
	statenassault = staten.filter(lambda x: x[1] == '351').map(lambda x: (x[0])).map(lambda x: (x,1)) \
					.reduceByKey(add).sortByKey().map(lambda x: "{0},{1}".format(x[0],x[1])).saveAsTextFile("staten-mischief.csv")				
	
	sc.stop()