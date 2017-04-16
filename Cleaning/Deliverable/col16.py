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

	def semantictype_occur(value):
		try: 
			name = value.upper()		
			if (len(name) > 5 and len(name) < 12 and type(name) == str):
				return "Premises Description"
			else:
				return "Other Semantic Type"
		except ValueError:	
				return "Other Semantic Type"		


	def validity_occur(x):
		x = x.upper()
		occur = ['FRONT OF', 'INSIDE', 'REAR OF', 'OUTSIDE', 'OPPOSITE OF']
		if (x == '' or x == ' '):
			return "NULL"
		elif x in occur:
			return "VALID"
		else:	
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[15]))
	
	column16 = lines.map(lambda x: (x, basetype_string(x), semantictype_occur(x), validity_occur(x)))
						
	column16.saveAsTextFile("column16.out")

	sc.stop()