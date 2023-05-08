# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def displayNulls(df):
    nullsDf = df.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in df.columns])
    return nullsDf

# COMMAND ----------

def createDataframe(file):    
    sep = "\t"
    header = True

    df = (spark.read.option("sep",sep)
               .option("header",header)
               .format('csv')
               .load(stagePath+file))
    return df

# COMMAND ----------

def writeDfToTable(df,tableName):
    (df.write.format('delta')
       .option("mergeSchema", "true")
       .mode('overwrite')
       .option('path',landingPath+tableName)
       .saveAsTable(tableName))