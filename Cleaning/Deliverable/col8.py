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

	def semantictype_desc(value):
		try: 
			name = value.upper()		
			if (len(name) > 3 and type(name) == str):
				return "Offense Description"
			else:
				return "Other"
		except ValueError:	
				return "Other"		
			
	def validity_desc(x):
		x = x.upper()
		if x == '':
			return "NULL"
		elif (len(x) > 3 and type(x) == str):
			return "VALID"
		else:	
			return "INVALID"
	
	def description(input):
		special = "/&"
		for i in special:
			for i in input:
				input.replace('i','and')
		return input	
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[7]))
	
	column8 = lines.map(lambda x: (description(x), basetype_string(x), semantictype_desc(x), validity_desc(x)))
						
	column8.saveAsTextFile("column8.out")

	sc.stop()