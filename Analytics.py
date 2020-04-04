# Databricks notebook source
import pandas as pd
import os
import matplotlib.pyplot as plt
import glob

# COMMAND ----------

#retrieving csv data from the github repo
%sh curl -O 'https://github.com/rrochlin/CapstoneData.git'

# COMMAND ----------

#checking save location
%fs ls "file:/databricks/driver"

# COMMAND ----------

#gathering csv data using glob
all_csv_files = glob.glob("/dbfs/FileStore/tables/*.csv")
print("--" + str(all_csv_files))
loops=len(all_csv_files)
all_csv_files

# COMMAND ----------

# https://docs.databricks.com/data/data.html
from os import walk

mypath = "/dbfs/FileStore/tables"
f = []
for (dirpath, dirnames, filenames) in walk(mypath):
    f.extend(filenames)
    break
print(f)

# COMMAND ----------

pd.read_csv("/dbfs/FileStore/tables/RTU_1_BldgStatPress_2019-343cc.csv")


# COMMAND ----------

# definition for TS2Sec, function for converting timestamp string into seconds.
def monthstime(month):
        if month =='Jan':  
            return 0
        elif month == 'Feb':  
            return 31
                     
        elif month ==  'Mar':  
            return 31+28
                   
        elif month == 'Apr':  
            return 31+28+31
                     
        elif month == 'May':  
            return 31+28+31+30
                     
        elif month == 'Jun':  
            return 31+28+31+30+31
                     
        elif month == 'Jul':  
            return 31+28+31+30+31+30
                     
        elif month == 'Aug':  
            return 31+28+31+30+31+30+31
                     
        elif month == 'Sep':  
            return 31+28+31+30+31+30+31+31
                     
        elif month == 'Oct': 
            return 31+28+31+30+31+30+31+31+30
                     
        elif month == 'Nov': 
            return 31+28+31+30+31+30+31+31+30+31
                     
        elif month == 'Dec': 
            return 31+28+31+30+31+30+31+31+30+31+30
                     
        



def TS2Sec(x):
    loops=len(x)
    for i in range(loops):
        total=0
        month=x.iat[i,0]
        #days month is dd-MMM-YY HH:mm:ss am or pm
        temp=month.split('-')[0]
        month=month.split('-')[1]+'-'+month.split('-')[2]
        total+=int(temp)*24*60**2
        #months month is MMM-YY HH:mm:ss am or pm
        temp=month.split('-')[0]
        month=month.split('-')[1]
        total+=monthstime(temp)*24*60**2
        #year month is YY HH:mm:ss am or pm
        temp=month.split(' ')[0]
        month=month.split(' ')[1]+' '+month.split(' ')[2]
        total+=(int(temp)-18)*365*24*60**2
        #hours month is HH:mm:ss am or pm
        temp=month.split(':')[0]
        month=month.split(':')[1]+':'+month.split(':')[2]
        total+=int(temp)*60**2
        #minutes month is mm:ss am or pm
        temp=month.split(':')[0]
        month=month.split(':')[1]
        total+=int(temp)*60
        #seconds month is ss am or pm
        temp=month.split(' ')[0]
        month=month.split(' ')[1]
        total+=int(temp)*60
        #am/pm month is am or pm
        if month == 'pm':
            total+=12*60**2
        x.iat[i,0]=total

# COMMAND ----------

varlist=[]
for i in range(loops):
    string=all_csv_files[i]
    string=string.split("RTU_1 ")[1]
    string1=string.split(" ")[0]
    string2=string.split(" ")[1]
    string2=string2.split(".")[0]
    if '-' in string2:
        string2=string2.replace('-','')
    globals() [string1+string2] = pd.read_csv(all_csv_files[i])
    varlist.append(string1+string2)
#for i in range(len(varlist)):
#    TS2Sec(globals()[varlist[i]])
#converts timestamps to seconds if needed

# COMMAND ----------

varlist

# COMMAND ----------

ax = plt.gca()

OutdoorTemp2019.plot(kind='line',x='Timestamp',y='RTU_1 OutdoorTemp(°F)',ax=ax)
RATemp2019.plot(kind='line',x='Timestamp',y='RTU_1 RATemp(°F)', color='red', ax=ax)
plt.xticks(rotation='vertical')

plt.show()
#here we're looking at retrun air vs outdoor temp
#anywhere where OutDoorTemp>RATemp we expect the 
#economizer to be active

# COMMAND ----------

import pandas as pd
import numpy as np

# generating some test data
timestamp = [1440540000, 1450540000]
df1 = pd.DataFrame(
    {'timestamp': timestamp, 'a': ['val_a', 'val2_a'], 'b': ['val_b', 'val2_b'], 'c': ['val_c', 'val2_c']})
print(df1)

# building a different index
timestamp = timestamp * np.random.randn(abs(1))
df2 = pd.DataFrame(
    {'timestamp': timestamp, 'd': ['val_d', 'val2_d'], 'e': ['val_e', 'val2_e'], 'f': ['val_f', 'val2_f'],
     'g': ['val_g', 'val2_g']}, index=timestamp)
print(df2)

# keeping a value in common with the first index
timestamp = [1440540000, 1450560000]
df3 = pd.DataFrame({'timestamp': timestamp, 'h': ['val_h', 'val2_h'], 'i': ['val_i', 'val2_i']}, index=timestamp)
print(df3)

# Setting the timestamp as the index
df1.set_index('timestamp', inplace=True)
df2.set_index('timestamp', inplace=True)
df3.set_index('timestamp', inplace=True)

# You can convert timestamps to dates but it's not mandatory I think
df1.index = pd.to_datetime(df1.index, unit='s')
df2.index = pd.to_datetime(df2.index, unit='s')
df3.index = pd.to_datetime(df3.index, unit='s')

# Just perform a join and that's it
result = df1.join(df2, how='outer').join(df3, how='outer')
result

# COMMAND ----------

