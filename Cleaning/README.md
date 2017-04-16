Notes on How To Run the Cleaning Scripts
-------------------
- Download the NYPD Complaint Data from [here.](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)

- To clean data and extract cleaned CSV file:
    spark-submit cleaning.py /user/hps257(username)/NYPD_Complaint_Data_Historic.csv

2. Last argument is the input file(NYPD Crime data) stored in the folder in Hadoop		

3. Output file will be written to Hadoop. To get output file type:

		hfs -getmerge cleaned.csv clean.csv

4. Copy the merged file back to hadoop to run analysis scripts on this cleaned CSV file

		Hadoop fs -copyFromLocal cleaned.csv

5. Spark command to execute deliverable scripts:

		spark-submit col1.py /user/hps257/NYPD_Complaint_Data_Historic.csv
	
*where col1.py is the spark script*

6. Output file will be written to Hadoop. To get output file type:

		hfs -getmerge column1.out column1.out

7. Below Aliases set in .bashrc file: 	
   
        alias hfs='/usr/bin/hadoop fs '
	
        export HAS=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib
	
        export HSJ=hadoop-mapreduce/hadoop-streaming.jar
	
        alias hjs='/usr/bin/hadoop jar $HAS/$HSJ'		
