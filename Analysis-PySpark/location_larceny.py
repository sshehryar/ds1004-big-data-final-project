from __future__ import print_function

import sys
from csv import reader
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))
	lines = lines.map(lambda x: (x[1], x[6], x[13], x[23]))

	bronx = lines.filter(lambda x: x[2] == 'BRONX').map(lambda x: (x[0][:2], x[1], x[3]))
	bronxlarcenyfeb = bronx.filter(lambda x: x[1] == '341').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("bronx-larceny-feb.csv")
	bronxlarcenysummer = bronx.filter(lambda x: x[1] == '341').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("bronx-larceny-summer.csv")
	bronxlarcenyall = bronx.filter(lambda x: x[1] == '341').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("bronx-larceny-all.csv")
	bronxassaultfeb = bronx.filter(lambda x: x[1] == '344').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("bronx-assault-feb.csv")
	bronxassaultsummer = bronx.filter(lambda x: x[1] == '344').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("bronx-assault-summer.csv")
	bronxassaultall = bronx.filter(lambda x: x[1] == '344').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("bronx-assault-all.csv")				
					
	brooklyn = lines.filter(lambda x: x[2] == 'BROOKLYN').map(lambda x: (x[0][:2], x[1], x[3]))
	brooklynlarcenyfeb = brooklyn.filter(lambda x: x[1] == '341').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '')\
					.saveAsTextFile("brooklyn-larceny-feb.csv")
	brooklynlarcenysummer = brooklyn.filter(lambda x: x[1] == '341').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("brooklyn-larceny-summer.csv")
	brooklynlarcenyall = brooklyn.filter(lambda x: x[1] == '341').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("brooklyn-larceny-all.csv")
	brooklynassaultfeb = brooklyn.filter(lambda x: x[1] == '344').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("brooklyn-assault-feb.csv")
	brooklynassaultsummer = brooklyn.filter(lambda x: x[1] == '344').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("brooklyn-assault-summer.csv")
	brooklynassaultall = brooklyn.filter(lambda x: x[1] == '344').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("brooklyn-assault-all.csv")				
	
	manhattan = lines.filter(lambda x: x[2] == 'MANHATTAN').map(lambda x: (x[0][:2], x[1], x[3]))
	manhattanlarcenyfeb = manhattan.filter(lambda x: x[1] == '341').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("manhattan-larceny-feb.csv")
	manhattanlarcenysummer = manhattan.filter(lambda x: x[1] == '341').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("manhattan-larceny-summer.csv")
	manhattanlarcenyall = manhattan.filter(lambda x: x[1] == '341').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("manhattan-larceny-all.csv")
	manhattangrandfeb = manhattan.filter(lambda x: x[1] == '109').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("manhattan-grand-feb.csv")
	manhattangrandsummer = manhattan.filter(lambda x: x[1] == '109').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("manhattan-grand-summer.csv")
	manhattangrandall = manhattan.filter(lambda x: x[1] == '109').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("manhattan-grand-all.csv")				
	
	queens = lines.filter(lambda x: x[2] == 'QUEENS').map(lambda x: (x[0][:2], x[1], x[3]))
	queenslarcenyfeb = queens.filter(lambda x: x[1] == '341').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("queens-larceny-feb.csv")
	queenslarcenysummer = queens.filter(lambda x: x[1] == '341').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("queens-larceny-summer.csv")
	queenslarcenyall = queens.filter(lambda x: x[1] == '341').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("queens-larceny-all.csv")
	queensassaultfeb = queens.filter(lambda x: x[1] == '344').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("queens-assault-feb.csv")
	queensassaultsummer = queens.filter(lambda x: x[1] == '344').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("queens-assault-summer.csv")
	queensassaultall = queens.filter(lambda x: x[1] == '344').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("queens-assault-all.csv")				
	
	staten = lines.filter(lambda x: x[2] == 'STATEN ISLAND').map(lambda x: (x[0][:2], x[1], x[3]))
	statenlarcenyfeb = staten.filter(lambda x: x[1] == '341').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("staten-larceny-feb.csv")
	statenlarcenysummer = staten.filter(lambda x: x[1] == '341').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("staten-larceny-summer.csv")
	statenlarcenyall = staten.filter(lambda x: x[1] == '341').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("staten-larceny-all.csv")
	statenassaultfeb = staten.filter(lambda x: x[1] == '351').filter(lambda x: x[0] == '02').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("staten-assault-feb.csv")
	statenassaultsummer = staten.filter(lambda x: x[1] == '351').filter(lambda x: x[0] in ('06', '07', '08')).map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("staten-assault-summer.csv")
	statenassaultall = staten.filter(lambda x: x[1] == '351').map(lambda x: (x[2])).filter(lambda x: x != '') \
					.saveAsTextFile("staten-assault-all.csv")				
	
	sc.stop()