from __future__ import print_function

import sys
import re
import string
import math
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	
	lines = lines.mapPartitions(lambda x: reader(x))
	
	def basetype_float(input):
		try:
			number = float(input)
			return "FLOAT"
		except ValueError:
			return type(input)
		
	class Point:
		def __init__(self,x,y):
			self.x = x
			self.y = y

	class Polygon:
		def __init__(self,points):
			self.points = points
			self.nvert = len(points)

			minx = maxx = points[0].x
			miny = maxy = points[0].y
			for i in range(1,self.nvert):
				minx = min(minx,points[i].x)
				miny = min(miny,points[i].y)
				maxx = max(maxx,points[i].x)
				maxy = max(maxy,points[i].y)

			self.bound = (minx,miny,maxx,maxy)


		def contains(self,pt):
			firstX = self.points[0].x
			firstY = self.points[0].y
			testx = pt.x
			testy = pt.y
			c = False
			j = 0
			i = 1
			nvert = self.nvert
			while (i < nvert) :
				vi = self.points[i]
				vj = self.points[j]

				if(((vi.y > testy) != (vj.y > testy)) and (testx < (vj.x - vi.x) * (testy - vi.y) / (vj.y - vi.y) + vi.x)):
									c = not(c)

				if(vi.x == firstX and vi.y == firstY):
					i = i + 1
					if (i < nvert):
						vi = self.points[i];
						firstX = vi.x;
						firstY = vi.y;
				j = i
				i = i + 1
			return c

		def bounds(self):
			return self.bound	
	

	polySTATENISLAND = Polygon([Point(40.640457, -74.124584),Point(40.637965, -74.157511),Point(40.635247, -74.193249),Point(40.596937, -74.199429),Point(40.557300, -74.215221),Point(40.507983, -74.255390),Point(40.494930, -74.237881),Point(40.546866, -74.112225),Point(40.599022, -74.052830),Point(40.646709, -74.075832),Point(40.640457, -74.124584)])
	polyBROOKLYN = Polygon([Point(40.736165, -73.961077),Point(40.682648, -73.898678),Point(40.646709, -73.851643),Point(40.615182, -73.889751),Point(40.581033, -73.877735),Point(40.573731, -73.896961),Point(40.571124, -74.011631),Point(40.618310, -74.050083),Point(40.662337, -74.017124),Point(40.679263, -74.023647),Point(40.706596, -73.991718),Point(40.708157, -73.973179),Point(40.736165, -73.961077)])
	polyQUEENS = Polygon([Point(40.736165, -73.961077),Point(40.682648, -73.898678),Point(40.646709, -73.851643),Point(40.593882, -73.824829),Point(40.554244, -73.947052),Point(40.537025, -73.942245),Point(40.537025, -73.942245),Point(40.593463, -73.749302),Point(40.656941, -73.720459),Point(40.750638, -73.703979),Point(40.801596, -73.822769),Point(40.776122, -73.935379),Point(40.736165, -73.961077)])
	polyMANHATTAN = Polygon([Point(40.679263, -74.023647),Point(40.706596, -73.991718),Point(40.708157, -73.973179),Point(40.736165, -73.961077),Point(40.796398, -73.924393),Point(40.842126, -73.929886),Point(40.870690, -73.904480),Point(40.875363, -73.927826),Point(40.750118, -74.013657),Point(40.679263, -74.023647)])
	polyBRONX = Polygon([Point(40.796398, -73.924393),Point(40.842126, -73.929886),Point(40.870690, -73.904480),Point(40.875363, -73.927826),Point(40.916291, -73.914413),Point(40.878088, -73.780472),Point(40.837840, -73.778412),Point(40.786222, -73.871622),Point( 40.783441, -73.885027),Point(40.793669, -73.906471),Point(40.796398, -73.924393)])
	
	def check(x):
		if x== "" or x== " ":
			return "Other, NULL"
		x=x.strip("'")
		x=x.replace("(","")
		x=x.replace(")","")
		try:
			lat,lon=x.split(",")
		except ValueError:
			return "Other, INVALID"
		lat=lat.strip()
		lon=lon.strip()
		lat=float(lat)
		lon=float(lon)
		if polySTATENISLAND.contains(Point(lat,lon)) or polyBRONX.contains(Point(lat,lon)) or polyMANHATTAN.contains(Point(lat,lon)) or polyQUEENS.contains(Point(lat,lon)) or polyBROOKLYN.contains(Point(lat,lon)):
			return "Location, VALID"
		else:
			return "Location, INVALID"
			
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[23]))
	
	column24 = lines.map(lambda x: (x, basetype_float(x), check(x)))
						
	column24.saveAsTextFile("column24.out")

	sc.stop()