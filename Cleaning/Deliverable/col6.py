from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	
	lines = lines.mapPartitions(lambda x: reader(x))
	
	def basetype_date(input):
		mat=re.match('(\d{2}|\d{1})/(\d{2}|\d{1})/(\d{4})$', input)
		if mat is not None:
			return "DATETIME"
		else:
			return type(input)
	

	def semantictype_date(value):
		mat=re.match('(\d{2}|0?[1-9])/(\d{2}|0?[1-9])/(\d{4})$', value)
		if mat is not None:
			return "Report Date"
		else:
			return "Other"	
		
	def validity_date(x):
		if x == '':
			return "NULL"
		try:
			if x != datetime.strptime(x, "%m/%d/%Y").strftime('%m/%d/%Y'):
				raise ValueError
			mat=re.match('(1[0-2]|0?[1-9])/(3[01]|[12][0-9]|0?[1-9])/(20)(0[6-9]|1[0-5])$', x)
			if mat is not None:
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[5]))
	
	column6 = lines.map(lambda x: (x, basetype_date(x), semantictype_date(x), validity_date(x)))
						
	column6.saveAsTextFile("column6.out")

	sc.stop()