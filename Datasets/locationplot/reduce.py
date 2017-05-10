#!/usr/bin/env python
# Reduce function for calculating top-20 vehicles
# in terms of most violations
# Task - 6

import sys
import string

currentkey = None
count = 0
mostviolated = dict()

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    #Remove leading and trailing whitespace
    line = line.strip()

    #Get key/value
    key = line

    #If we are still on the same key...
    if key==currentkey:

        #Process key/value pair (your code goes here)
        count += 1

				
    #Otherwise, if this is a new key...
    else:
        #If this is a new key and not the first key we've seen
        if currentkey:

            #compute result (your code goes here)
            mostviolated[currentkey] = count

        currentkey = key

        #Process input for new key (your code goes here)
        count = 1

#Compute result for the last key (your code goes here)
mostviolated[currentkey] = count

#Compute/output result to STDOUT
output=dict(sorted(mostviolated.items(), key=lambda x: x[1],reverse=True)[:10])
	
for k,v in output.items():
    k = k.strip().split(",")
    print(str(k).replace("'","").replace('[','').replace(']','') + '\t' + str(v))
