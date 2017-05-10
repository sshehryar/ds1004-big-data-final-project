#!/usr/bin/env python
# Map function for calculating top-20 vehicles
# in terms of most violations
# Task - 6

import sys
import string
import csv

# input comes from STDIN (stream data from csv file that goes to the program)
for line in sys.stdin:

	#Remove leading and trailing whitespace
	line = line.strip()
	
	#Parse columns from single csv row
	row = csv.reader([line], delimiter=',')	
	row = list(row)[0]
	
	#Generate the necessary key-value pairs
	print(str(row[0]) + "," + str(row[1]))
