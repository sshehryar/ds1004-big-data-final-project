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
	
	def basetype_int(input):
		try:
			number = int(input)
			return "INT"
		except ValueError:
			return type(input)
			
	def semantictype_pct(x):
		try: 
			if (len(x) < 4 and len(x) >= 1 and x.isdigit()):
				return "Address Precinct"
			else:
				return "Other Semantic Type"
		except ValueError:	
				return "Other Semantic Type"	
			
	def validity_pct(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) < 4 and len(x) >= 1 and x.isdigit() and int(x) > 0 and int(x) < 130):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[14]))
	
	column15 = lines.map(lambda x: (x, basetype_int(x), semantictype_pct(x), validity_pct(x)))
						
	column15.saveAsTextFile("column15.out")

	sc.stop()