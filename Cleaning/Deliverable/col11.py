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

	def semantictype_crm(value):
		try: 
			name = value.upper()		
			if (len(name) == 9 and type(name) == str):
				return "Crime Culmination"
			else:
				return "Other Semantic Type"
		except ValueError:	
				return "Other Semantic Type"

	def validity_crm(x):
		x = x.upper()
		crm = ['COMPLETED', 'ATTEMPTED']
		if x == '':
			return "NULL"
		elif x in crm:
			return "VALID"
		else:	
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[10]))
	
	column11 = lines.map(lambda x: (x, basetype_string(x), semantictype_crm(x), validity_crm(x)))
						
	column11.saveAsTextFile("column11.out")	

	sc.stop()