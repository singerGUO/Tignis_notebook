# Databricks notebook source
import json
import requests
import base64
from azure.core.paging import ItemPaged
from azure.storage.blob import ContainerClient, BlobProperties, StorageStreamDownloader


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Blob storage.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Set the data location and type
# MAGIC 
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC 
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

storage_account_name = "jonazureiothubstorage"
storage_account_access_key = "OuAD1TDqf2IeWztRgjORWhfUrCTAxyuBHDwg9xGSLYY3FC1uirDPAyIVk1JpvarR5OR7rRMIAexxcG2+ZBRgOQ=="

# storage_account_name = "tignisstorage8811"
# storage_account_access_key = "5RCHiJjzkPhDVR6lcIWWKE4Z3mB+XF0k5KVXgppB29wB51G3lnB16As23K4hqwKgWThjvX4bTLHR0JOH1IoKYA=="

# COMMAND ----------

#file_location = "wasbs://joniothubtest@jonazureiothubstorage.blob.core.windows.net/TignisIhub/01/2020/04/30/23/17"
#file_location = "wasbs://joniothubtest@jonazureiothubstorage.blob.core.windows.net/TignisIhub/01/2020/05/01/03/34"
file_location = "wasbs://joniothubtest@jonazureiothubstorage.blob.core.windows.net/TignisIhub/01/2020/05/01/03"
file_type =  'json'

# file_location = "wasbs://tignisroutingresultscontainer@tignisstorage8811.blob.core.windows.net/TignisResourceTestIoTHub8811/02/2020/04/29/08/"
# file_type =  'json'



# COMMAND ----------




spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# dbutils.fs.mount(
#   source = "wasbs://tignisroutingresultscontainer@tignisstorage8811.blob.core.windows.net",
#   mount_point = "/mnt/tignisroutingresultscontainer",
#   extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net": storage_account_access_key})

# COMMAND ----------

# dbutils.fs.ls("/mnt/tignisroutingresultscontainer")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Read the data
# MAGIC 
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC 
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------


#First try
# df1 = spark.read.format(file_type)\
# .option("inferSchema", "true")\
# .option("multiLine", "true")\
# .load(file_location)


#second try
#df = spark.read.json(file_location, multiLine=True)
df = spark.read.json(file_location)
#third try

# with open('/dbfs/FileStore/tables/25.json', 'r',encoding = "ISO-8859-1") as f:
#     dict = json.load(f)
    
    


#fourth try
#spark.read.option("charset", "UTF-8").json(file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Query the data
# MAGIC 
# MAGIC Now that we have created our DataFrame, we can query it. For instance, you can identify particular columns to select and display.

# COMMAND ----------

#display(df)
#display(df.select("Body"))
b2=df.select("Body").collect()

decoded_list=[]



for i in range(len(b2)):
  body=b2[i].__getitem__("Body")
  decoded_once_body = base64.b64decode(body)
  decoded_twice_body = decoded_once_body.decode("utf-8")
  decoded_list.append(decoded_twice_body)
  

sc = spark.sparkContext
jsonStrings = decoded_list

hvacRDD = sc.parallelize(jsonStrings)
hvacdata = spark.read.json(hvacRDD)
hvacdata.show()


# try:
#     original_mqtt_struct: dict = json.loads(decoded_twice_body)
# except JSONDecodeError:
#   print("Message was not valid JSON format")
# if 'message' in original_mqtt_struct and 'timestamp' in original_mqtt_struct:
#   print("message: {} and timestamp: {}".format(original_mqtt_struct['message'],
#                                                          original_mqtt_struct['timestamp']))
# else:
#   print("JSON record found " + str(original_mqtt_struct))



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC 
# MAGIC If you want to query this data as a table, you can simply register it as a *view* or a table.

# COMMAND ----------

print('Register the DataFrame as a SQL temporary view: source')
df.createOrReplaceTempView("source")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can query this view using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT EXAMPLE_GROUP, SUM(EXAMPLE_AGG) FROM YOUR_TEMP_VIEW_NAME GROUP BY EXAMPLE_GROUP

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

df.write.format("parquet").saveAsTable("MY_PERMANENT_TABLE_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data.