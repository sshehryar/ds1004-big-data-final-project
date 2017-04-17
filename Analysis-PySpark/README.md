Notes on How To Run the Analysis Scripts
-------------------

- To clean data and extract cleaned CSV file:

       spark-submit total_offenses_borough_ month_year.py /user/hps257(username)/clean.csv
*Last argument is the input file stored in the folder in Hadoop. Follow the same process for the other four scripts.*		

3. Output file will be written to Hadoop. To get output file type:

		hfs -getmerge total_offenses_month_year_(*Borough Name Goes Here*).csv total_offenses_month_year_(*Borough Name Goes Here*).csv

*Similarly, you can execute for **total_offenses_year.py*** and ***total_offenses_month***.

4. For overall analysis result on borough, run:

        hfs -getmerge total_offenses_borough.csv total_offenses_borough.csv
*Similarly, you can execute for total_offenses.csv*.
