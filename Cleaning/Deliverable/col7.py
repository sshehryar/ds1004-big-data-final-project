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
	

	def semantictype_cd(x):
		try: 
			if (len(x) == 3) and x.isdigit():
				return "Offense Key Code"
			else:
				return "Other"
		except ValueError:	
				return "Other"	
		
	def validity_key_cd(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 900):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[6]))
	
	column7 = lines.map(lambda x: (x, basetype_int(x), semantictype_cd(x), validity_key_cd(x)))
						
	column7.saveAsTextFile("column7.out")

	sc.stop()