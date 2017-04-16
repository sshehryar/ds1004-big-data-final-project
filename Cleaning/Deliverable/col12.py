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

	def semantictype_law(value):
		try: 
			name = value.upper()		
			if (len(name) > 5 and len(name) < 12 and type(name) == str):
				return "Offense Level name"
			else:
				return "Other"
		except ValueError:	
				return "Other"

	def validity_law(x):
		x = x.upper()
		law = ['MISDEMEANOR', 'VIOLATION', 'FELONY']
		if x == '':
			return "NULL"
		elif x in law:
			return "VALID"
		else:	
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[11]))
	
	column12 = lines.map(lambda x: (x, basetype_string(x), semantictype_law(x), validity_law(x)))
						
	column12.saveAsTextFile("column12.out")

	sc.stop()