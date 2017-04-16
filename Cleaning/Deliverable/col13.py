from __future__ import print_function

import sys
import string
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	
	lines = lines.mapPartitions(lambda x: reader(x))
	
	def basetype_string(input):
		try:
			if type(input) == str:
				return "STRING"
		except ValueError:
			return type(input)

	def semantictype_juris(value):
		try: 
			name = value.upper()		
			if (len(name) > 4 and type(name) == str):
				return "Juris_Desc"
			else:
				return "Other"
		except ValueError:	
				return "Other"		
			
	def validity_juris(x):
		x = x.upper()
		if x == '':
			return "NULL"
		elif (len(x) > 4 and type(x) == str):
			return "VALID"
		else:	
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[12]))
	
	column13 = lines.map(lambda x: (x, basetype_string(x), semantictype_juris(x), validity_juris(x)))
						
	column13.saveAsTextFile("column13.out")	

	sc.stop()