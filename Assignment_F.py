# Databricks notebook source
display(dbutils.fs.ls("/FileStore/tables/"))

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df = pd.read_csv("/dbfs/FileStore/tables/c914dbbk1508907717096/Zip_Zhvi_Summary_AllHomes.csv")

# COMMAND ----------

df

# COMMAND ----------

sparkDF = sqlContext.read.format("csv").load("/FileStore/tables/77s9zkp11508989320358/data_homes.csv",header=None)

# COMMAND ----------

display(sparkDF)

# COMMAND ----------

dataPath = "/FileStore/tables/77s9zkp11508989320358/data_homes.csv"
loadData = sqlContext.read.format("com.databricks.spark.csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(dataPath)

# COMMAND ----------

display(loadData)

# COMMAND ----------

df1 = loadData.groupBy("SizeRank", "State").avg("Zhvi")

# COMMAND ----------

df1.count()

# COMMAND ----------

display(df1)

# COMMAND ----------

df2 = df1\
  .join(loadData, on='State', how='inner')\
  .select("`avg(Zhvi)`","5Year","10Year","City")

# COMMAND ----------

df2.count()

# COMMAND ----------

display(df2)

# COMMAND ----------

df3 = loadData.groupBy("PeakMonth", "PeakQuarter","State","Date").avg("PeakZHVI")

# COMMAND ----------

display(df3)

# COMMAND ----------

df4 =loadData.groupBy("State","Date","City").avg("PctFallFromPeak")

# COMMAND ----------

display(df4)

# COMMAND ----------

df5 = df4\
  .join(df3, on='State', how='inner')\
  .select("`avg(PeakZHVI)`","`avg(PctFallFromPeak)`","State")

# COMMAND ----------

display(df5)
