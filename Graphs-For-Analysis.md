

```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
plt.style.use('ggplot')
%matplotlib inline
%matplotlib notebook
#Check path for read csv. Should be path/date-borough-code-cleaned.csv
df = pd.read_csv("//Users/syedalishehryar/OneDrive/Big Data/Final_Project_Part1/ds1004-big-data-final-project/Data/date-borough-code-cleaned.csv", parse_dates=['CMPLNT_FR_DT'])
df['YEAR'] = df['CMPLNT_FR_DT'].dt.year
df['MONTH'] = df['CMPLNT_FR_DT'].dt.month

```


```python
# 3 top crimes in Brooklyn
df[df.BORO_NM=="BROOKLYN"].KY_CD.value_counts().nlargest(3)
# 341, 578, 344

df2 = df[df.BORO_NM == "BROOKLYN"]
dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("brooklyn-yearly-341.png")
plt.show()
plt.clf()


dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("brooklyn-yearly-578.png")
plt.show()
plt.clf()

dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("brooklyn-yearly-344.png")
plt.show()
plt.clf()


```


    <matplotlib.figure.Figure at 0x111f732d0>



    <matplotlib.figure.Figure at 0x1098c6b90>



    <matplotlib.figure.Figure at 0x11258f950>



    <matplotlib.figure.Figure at 0x111bca6d0>



    <matplotlib.figure.Figure at 0x1126ce410>



    <matplotlib.figure.Figure at 0x1126b8510>



![png](output_1_6.png)



    <matplotlib.figure.Figure at 0x111feb450>



![png](output_1_8.png)



    <matplotlib.figure.Figure at 0x1133314d0>



![png](output_1_10.png)



```python
# 3 top crimes in Bronx
df[df.BORO_NM=="BRONX"].KY_CD.value_counts().nlargest(3)
# 341, 578, 344

df2 = df[df.BORO_NM == "BRONX"]
dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest.plot()
plt.show()
plt.clf()

dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("bronx-yearly-578.png")
plt.show()
plt.clf()

dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("bronx-yearly-344.png")
plt.show()
plt.clf()


```


    <matplotlib.figure.Figure at 0x112bad050>



![png](output_2_1.png)



    <matplotlib.figure.Figure at 0x112224ed0>



![png](output_2_3.png)



    <matplotlib.figure.Figure at 0x111f730d0>



![png](output_2_5.png)



```python
# 3 top crimes in Queens
df[df.BORO_NM=="QUEENS"].KY_CD.value_counts().nlargest(3)
#341, 578, 344

df2 = df[df.BORO_NM == "QUEENS"]
dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("queens-yearly-341.png")
plt.show()
plt.clf()

dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("queens-yearly-578.png")
plt.show()
plt.clf()

dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==344)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("queens-yearly-344.png")
plt.show()
plt.clf()

```


    <matplotlib.figure.Figure at 0x111ffc910>



![png](output_3_1.png)



    <matplotlib.figure.Figure at 0x1129d4ad0>



![png](output_3_3.png)



    <matplotlib.figure.Figure at 0x1125eb1d0>



![png](output_3_5.png)



```python
# 3 top crimes in Manhattan
df[df.BORO_NM=="MANHATTAN"].KY_CD.value_counts().nlargest(3)
# 341, 109, 578


df2 = df[df.BORO_NM == "MANHATTAN"]
dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==341)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("manhattan-yearly-341.png")
plt.show()
plt.clf()

dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==578)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("manhattan-yearly-578.png")
plt.show()
plt.clf()

dfTest = pd.DataFrame(index = [1,2,3,4,5,6,7,8,9,10,11,12])
dfTest['2006'] = df2[(df2.YEAR==2006) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest['2007'] = df2[(df2.YEAR==2007) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest['2008'] = df2[(df2.YEAR==2008) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest['2009'] = df2[(df2.YEAR==2009) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest['2010'] = df2[(df2.YEAR==2010) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest['2011'] = df2[(df2.YEAR==2011) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest['2012'] = df2[(df2.YEAR==2012) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest['2013'] = df2[(df2.YEAR==2013) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest['2014'] = df2[(df2.YEAR==2014) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest['2015'] = df2[(df2.YEAR==2015) & (df2.KY_CD==109)].MONTH.value_counts().sort_index()
dfTest.plot()
#plt.savefig("manhattan-yearly-109.png")
plt.show()
plt.clf()

```


    <matplotlib.figure.Figure at 0x112534350>



![png](output_4_1.png)



    <matplotlib.figure.Figure at 0x1125f0fd0>



![png](output_4_3.png)



    <matplotlib.figure.Figure at 0x1124ea8d0>



![png](output_4_5.png)



```python
df['YEAR'] = df['CMPLNT_FR_DT'].dt.year
df['MONTH'] = df['CMPLNT_FR_DT'].dt.month
```


```python
# 3 top crimes in Brooklyn
df[df.BORO_NM=="BROOKLYN"].KY_CD.value_counts().nlargest(3)
# 341, 578, 344

df2 = df[df.BORO_NM == "BROOKLYN"]
dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Petit Larceny'] = df2[(df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Harassment'] = df2[(df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Assault & Related Offenses'] = df2[(df2.KY_CD==344)].YEAR.value_counts().sort_index()
dfTest['Total Crime'] = df2.YEAR.value_counts().sort_index()
ax = dfTest.plot(secondary_y=['Total Crime'], figsize=(10,7))
ax.set_title('BROOKLYN')
ax.set_ylabel('Number of Incidents per Offense')
ax.right_ax.set_ylabel('Total Number of Incidents')
#plt.savefig("./yearly-trends/brooklyn-top3-yearly-trend.png")
plt.show()
plt.clf()
```


    <matplotlib.figure.Figure at 0x112b15a90>



![png](output_6_1.png)



```python
# 3 top crimes in Bronx
df[df.BORO_NM=="BRONX"].KY_CD.value_counts().nlargest(3)
# 341, 578, 344

df2 = df[df.BORO_NM == "BRONX"]
dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Petit Larceny'] = df2[(df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Harassment'] = df2[(df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Assault & Related Offenses'] = df2[(df2.KY_CD==344)].YEAR.value_counts().sort_index()
dfTest['Total Crime'] = df2.YEAR.value_counts().sort_index()
ax = dfTest.plot(secondary_y=['Total Crime'], figsize=(10,7))
ax.set_title('BRONX')
ax.set_ylabel('Number of Incidents per Offense')
ax.right_ax.set_ylabel('Total Number of Incidents')
#plt.savefig("./yearly-trends/bronx-top3-yearly-trend.png")
plt.show()
plt.clf()

```


    <matplotlib.figure.Figure at 0x112282190>



![png](output_7_1.png)



```python
# 3 top crimes in Queens
df[df.BORO_NM=="QUEENS"].KY_CD.value_counts().nlargest(3)
#341, 578, 344

df2 = df[df.BORO_NM == "QUEENS"]
dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Petit Larceny'] = df2[(df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Harassment'] = df2[(df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Assault & Related Offenses'] = df2[(df2.KY_CD==344)].YEAR.value_counts().sort_index()
dfTest['Total Crime'] = df2.YEAR.value_counts().sort_index()
ax = dfTest.plot(secondary_y=['Total Crime'], figsize=(10,7))
ax.set_title('QUEENS')
ax.set_ylabel('Number of Incidents per Offense')
ax.right_ax.set_ylabel('Total Number of Incidents')
#plt.savefig("./yearly-trends/queens-top3-yearly-trend.png")
plt.show()
plt.clf()

```


    <matplotlib.figure.Figure at 0x112678a90>



![png](output_8_1.png)



```python
# 3 top crimes in Manhattan
df[df.BORO_NM=="MANHATTAN"].KY_CD.value_counts().nlargest(3)
# 341, 109, 578

df2 = df[df.BORO_NM == "MANHATTAN"]
dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Petit Larceny'] = df2[(df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Harassment'] = df2[(df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Grand Larceny'] = df2[(df2.KY_CD==109)].YEAR.value_counts().sort_index()
dfTest['Total Crime'] = df2.YEAR.value_counts().sort_index()
ax = dfTest.plot(secondary_y=['Total Crime'], figsize=(10,7))
ax.set_title('MANHATTAN')
ax.set_ylabel('Number of Incidents per Offense')
ax.right_ax.set_ylabel('Total Number of Incidents')
#plt.savefig("./yearly-trends/manhattan-top3-yearly-trend.png")
plt.show()
plt.clf()
```


    <matplotlib.figure.Figure at 0x113313350>



![png](output_9_1.png)



```python
# 3 top crimes in Staten Island
df[df.BORO_NM=="STATEN ISLAND"].KY_CD.value_counts().nlargest(3)
# 578, 341, 351

df2 = df[df.BORO_NM == "STATEN ISLAND"]
dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Harassment'] = df2[(df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Petit Larceny'] = df2[(df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Criminal Mischief & Related Offenses'] = df2[(df2.KY_CD==351)].YEAR.value_counts().sort_index()
dfTest['Total Crime'] = df2.YEAR.value_counts().sort_index()
ax = dfTest.plot(secondary_y=['Total Crime'], figsize=(10,7))
ax.set_title('STATEN ISLAND')
ax.set_ylabel('Number of Incidents per Offense')
ax.right_ax.set_ylabel('Total Number of Incidents')
#plt.savefig("./yearly-trends/si-top3-yearly-trend.png")
plt.show()
plt.clf()
```


    <matplotlib.figure.Figure at 0x112b15850>



![png](output_10_1.png)



```python
# BROOKLYN
df[df.BORO_NM=="BROOKLYN"].KY_CD.value_counts().nlargest(1)
# 341
df2 = df[df.BORO_NM == "BROOKLYN"]

dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Jan'] = df2[(df2.MONTH==1) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Feb'] = df2[(df2.MONTH==2) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Mar'] = df2[(df2.MONTH==3) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Apr'] = df2[(df2.MONTH==4) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['May'] = df2[(df2.MONTH==5) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Jun'] = df2[(df2.MONTH==6) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Jul'] = df2[(df2.MONTH==7) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Aug'] = df2[(df2.MONTH==8) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Sep'] = df2[(df2.MONTH==9) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Oct'] = df2[(df2.MONTH==10) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Nov'] = df2[(df2.MONTH==11) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Dec'] = df2[(df2.MONTH==12) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
ax = dfTest.plot(figsize=(10,7))
ax.set_title('BROOKLYN (TOP KY_CD = 341 | Petit Larceny)')
ax.set_ylabel('Number of Incidents')
#plt.savefig("./monthly/brooklyn-monthly-341.png")
plt.show()
plt.clf()

```


    <matplotlib.figure.Figure at 0x112bf1e50>



![png](output_11_1.png)



```python
# BRONX
df[df.BORO_NM=="BRONX"].KY_CD.value_counts().nlargest(1)
# 341
df2 = df[df.BORO_NM == "BRONX"]

dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Jan'] = df2[(df2.MONTH==1) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Feb'] = df2[(df2.MONTH==2) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Mar'] = df2[(df2.MONTH==3) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Apr'] = df2[(df2.MONTH==4) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['May'] = df2[(df2.MONTH==5) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Jun'] = df2[(df2.MONTH==6) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Jul'] = df2[(df2.MONTH==7) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Aug'] = df2[(df2.MONTH==8) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Sep'] = df2[(df2.MONTH==9) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Oct'] = df2[(df2.MONTH==10) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Nov'] = df2[(df2.MONTH==11) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Dec'] = df2[(df2.MONTH==12) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
ax = dfTest.plot(figsize=(10,7))
ax.set_title('BRONX (TOP KY_CD = 341 | Petit Larceny)')
ax.set_ylabel('Number of Incidents')
#plt.savefig("./monthly/bronx-monthly-341.png")
plt.show()
plt.clf()

```


    <matplotlib.figure.Figure at 0x112bf1ed0>



![png](output_12_1.png)



```python
# QUEENS
df[df.BORO_NM=="QUEENS"].KY_CD.value_counts().nlargest(1)
# 341
df2 = df[df.BORO_NM == "QUEENS"]

dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Jan'] = df2[(df2.MONTH==1) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Feb'] = df2[(df2.MONTH==2) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Mar'] = df2[(df2.MONTH==3) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Apr'] = df2[(df2.MONTH==4) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['May'] = df2[(df2.MONTH==5) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Jun'] = df2[(df2.MONTH==6) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Jul'] = df2[(df2.MONTH==7) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Aug'] = df2[(df2.MONTH==8) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Sep'] = df2[(df2.MONTH==9) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Oct'] = df2[(df2.MONTH==10) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Nov'] = df2[(df2.MONTH==11) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Dec'] = df2[(df2.MONTH==12) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
ax = dfTest.plot(figsize=(10,7))
ax.set_title('QUEENS (TOP KY_CD = 341 | Petit Larceny)')
ax.set_ylabel('Number of Incidents')
#plt.savefig("./monthly/queens-monthly-341.png")
plt.show()
plt.clf()

```


    <matplotlib.figure.Figure at 0x1125c9990>



![png](output_13_1.png)



```python
# MANHATTAN
df[df.BORO_NM=="MANHATTAN"].KY_CD.value_counts().nlargest(1)
# 341
df2 = df[df.BORO_NM == "MANHATTAN"]

dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Jan'] = df2[(df2.MONTH==1) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Feb'] = df2[(df2.MONTH==2) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Mar'] = df2[(df2.MONTH==3) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Apr'] = df2[(df2.MONTH==4) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['May'] = df2[(df2.MONTH==5) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Jun'] = df2[(df2.MONTH==6) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Jul'] = df2[(df2.MONTH==7) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Aug'] = df2[(df2.MONTH==8) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Sep'] = df2[(df2.MONTH==9) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Oct'] = df2[(df2.MONTH==10) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Nov'] = df2[(df2.MONTH==11) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
dfTest['Dec'] = df2[(df2.MONTH==12) & (df2.KY_CD==341)].YEAR.value_counts().sort_index()
ax = dfTest.plot(figsize=(10,7))
ax.set_title('MANHATTAN (TOP KY_CD = 341 | Petit Larceny)')
ax.set_ylabel('Number of Incidents')
#plt.savefig("./monthly/manhattan-monthly-341.png")
plt.show()
plt.clf()

```


    <matplotlib.figure.Figure at 0x112100090>



![png](output_14_1.png)



```python
# STATEN ISLAND
df[df.BORO_NM=="STATEN ISLAND"].KY_CD.value_counts().nlargest(1)
# 578
df2 = df[df.BORO_NM == "STATEN ISLAND"]

dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Jan'] = df2[(df2.MONTH==1) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Feb'] = df2[(df2.MONTH==2) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Mar'] = df2[(df2.MONTH==3) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Apr'] = df2[(df2.MONTH==4) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['May'] = df2[(df2.MONTH==5) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Jun'] = df2[(df2.MONTH==6) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Jul'] = df2[(df2.MONTH==7) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Aug'] = df2[(df2.MONTH==8) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Sep'] = df2[(df2.MONTH==9) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Oct'] = df2[(df2.MONTH==10) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Nov'] = df2[(df2.MONTH==11) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
dfTest['Dec'] = df2[(df2.MONTH==12) & (df2.KY_CD==578)].YEAR.value_counts().sort_index()
ax = dfTest.plot(figsize=(10,7))
ax.set_title('STATEN ISLAND (TOP KY_CD = 578 | Harassment)')
ax.set_ylabel('Number of Incidents')
#plt.savefig("./monthly/si-monthly-578.png")
plt.show()
plt.clf()

```


![png](output_15_0.png)



```python

```
