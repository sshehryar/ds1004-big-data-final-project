from __future__ import print_function

import sys
import re
import string
import time
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	
	lines = lines.mapPartitions(lambda x: reader(x))
	
	def basetype_time(input):
		mat=re.match('(\d{2}|\d{1}):(\d{2}|\d{1}):(\d{2}|\d{1})$', input)
		if mat is not None:
			return "DATETIME"
		else:
			return type(input)
		
	def semantictype_time(value):
		mat=re.match('(\d{2}|0?[1-9]):(\d{2}|0?[1-9]):(\d{2}|0?[1-9])$', value)
		if mat is not None:
			return "Complaint Time"
		else:
			return "Other"	
		
	def validity_time(x):
		if x == '':
			return "NULL"
		try: 
			if time.strptime(x, "%H:%M:%S"):
				mat=re.match('([01][0-9]|2[0-3]|0?[1-9]):([0-5][0-9]|0?[1-9]):([0-5][0-9]|0?[1-9])$', x)
				if mat is not None:
					return "VALID"
				else:	
					raise ValueError
		except ValueError:
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[2]))
	
	column3 = lines.map(lambda x: (x, basetype_time(x), semantictype_time(x), validity_time(x)))
						
	column3.saveAsTextFile("column3.out")

	sc.stop()