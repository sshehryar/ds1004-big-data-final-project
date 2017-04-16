DS 1004 - Big Data - Project
----------------------
**Team Members** 

Name    | NetID
-------- | ---
Burak Dincer | bd1308
Harshit Prabhat Singh    | hps257
Syed Ali Shehryar     | sas786


Analyzing NYPD Complaint Data (Historic)
-------------
#### <i class="icon-file"></i> Project Report Link
You can open the project report from <i class="icon-provider-gdrive"></i> **Google Drive** by clicking [here!](https://drive.google.com/open?id=1uCckvtVk8lKK7W41oE9Yu4Hsa5-1diJeJO_b9emTfVc)

-------------

We analyzed the number  of reported criminal offenses with respect to Boroughs that form New York City. To do so, we extracted three columns of our interest from the main data set: 

- **CMPLNT_FR_DT** (Exact date of occurrence for the reported event).

- **KY_CD** (Three digit offense classification code)

- **BORO_NM** (Name of Borough). 

-------------
With the help of these 3 columns, we analyzed monthly and yearly variations in the most frequently reported crimes for each of the **five** boroughs of New York City.

-------------
### IPYTHON NOTEBOOKS
We used Jupyter Notebooks to organize our plots and do preliminary analysis on the dataset of our interest (date-borough-code-cleaned.csv) that we extracted from the main NYPD Complaint Dataset. For proper exploration, we used Pyspark.
- Complaint-Analysis-Borough-Wise.ipynb lists the python scripts we used for a practice exploratory run along with the outputs.
- Graphs-for-Analysis.ipynb contains the code and plots that we have used to visualize trends of our interest.
