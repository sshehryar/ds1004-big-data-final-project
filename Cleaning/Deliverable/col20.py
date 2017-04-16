from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	
	lines = lines.mapPartitions(lambda x: reader(x))
	
	def basetype_int(input):
		try:
			number = int(input)
			return "INT"
		except ValueError:
			return type(input)
	

	def semantictype_number(value):
		mat=re.match('\d*', value)
		if mat is not None:
			return "Coordinate"
		else:
			return "Other"	
		
	def validity_number(x):
		if x == '':
			return "NULL"
		mat=re.match('\d*', x)
		if mat is not None:
			return "VALID"
		else:
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[19]))
	
	column20 = lines.map(lambda x: (x, basetype_int(x), semantictype_number(x), validity_number(x)))
						
	column20.saveAsTextFile("column20.out")

	sc.stop()