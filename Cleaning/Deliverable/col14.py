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

	def semantictype_boro(value):
		try: 
			name = value.upper()		
			if (len(name) > 4 and len(name) < 14 and type(name) == str):
				return "Boro name"
			else:
				return "Other"
		except ValueError:	
				return "Other"

	def validity_boro(x):
		x = x.upper()
		boroughs = ['BROOKLYN', 'STATEN ISLAND', 'MANHATTAN', 'QUEENS', 'BRONX']
		if x == '':
			return "NULL"
		elif x in boroughs:
			return "VALID"
		else:	
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[13]))
	
	column14 = lines.map(lambda x: (x, basetype_string(x), semantictype_boro(x), validity_boro(x)))
						
	column14.saveAsTextFile("column14.out")

	sc.stop()