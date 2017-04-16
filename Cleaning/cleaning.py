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
	
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[1], x[6], x[13]))
	
	def basetype_date(input):
		mat=re.match('(\d{2}|\d{1})/(\d{2}|\d{1})/(\d{4})$', input)
		if mat is not None:
			return "DATETIME"
		else:
			return type(input)

	def basetype_string(input):
		try:
			if type(input) == str:
				return "STRING"
		except ValueError:
			return type(input)		

	def basetype_int(input):
		try:
			number = int(input)
			return "INT"
		except ValueError:
			return type(input)
			
	def semantictype_date(value):
		mat=re.match('(\d{2}|0?[1-9])/(\d{2}|0?[1-9])/(\d{4})$', value)
		if mat is not None:
			return "Complaint Date"
		else:
			return "Other"

	def semantictype_boro(value):
		try: 
			name = value.upper()		
			if (len(name) > 4 and len(name) < 14 and type(name) == str):
				return "Boro name"
			else:
				return "Other"
		except ValueError:	
				return "Other"			
 
	def semantictype_cd(x):
		try: 
			if (len(x) == 3) and x.isdigit():
				return "Offense Key Code"
			else:
				return "Other"
		except ValueError:	
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
		
	def validity_boro(x):
		x = x.upper()
		boroughs = ['BROOKLYN', 'STATEN ISLAND', 'MANHATTAN', 'QUEENS', 'BRONX']
		if x == '':
			return "NULL"
		elif x in boroughs:
			return "VALID"
		else:	
			return "INVALID"

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

	def toCSVLine(data):
	  return ','.join(str(d) for d in data)			
	
	deliverable = lines.map(lambda x: (x[0], basetype_date(x[0]), semantictype_date(x[0]), validity_date(x[0]),\
						x[1], basetype_int(x[1]), semantictype_cd(x[1]), validity_key_cd(x[1]), \
						x[2], basetype_string(x[2]), semantictype_boro(x[2]), validity_boro(x[2])))

	result = deliverable.filter(lambda x: x[3] == "VALID" and x[7] == "VALID" and x[11]== "VALID") \
			.map(lambda x: (x[0], x[4], x[8]))
	
	cleaned = result.map(toCSVLine)
	
	cleaned.saveAsTextFile("cleaned.csv")
	
	sc.stop()