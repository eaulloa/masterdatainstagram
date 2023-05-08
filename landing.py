# Databricks notebook source
adlsAccountName  = 'adlsAccountName'
sourceContainer = 'sourceContainer'
destinationContainer = 'destinationContainer'

dbutils.widgets.text(name=adlsAccountName, defaultValue="", label="adlsAccountName")
adlsAccountName = str(dbutils.widgets.get(adlsAccountName))

dbutils.widgets.text(name=sourceContainer, defaultValue="", label="sourceContainer")
sourceContainer = str(dbutils.widgets.get(sourceContainer))

dbutils.widgets.text(name=destinationContainer, defaultValue="", label="destinationContainer")
destinationContainer = str(dbutils.widgets.get(destinationContainer))

# COMMAND ----------

stagePath = f'abfss://{sourceContainer}@{adlsAccountName}.dfs.core.windows.net/datasets/'
landingPath = f'abfss://{destinationContainer}@{adlsAccountName}.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %run ../utils/functions

# COMMAND ----------

dictDfs = {f'{file.name[:-4]}Df': createDataframe(file.name) for file in dbutils.fs.ls(stagePath)}

# COMMAND ----------

for table,df in zip(list(dictDfs.keys()),dictDfs.values()):
    tableName = table[:-2] + '_raw'
    writeDfToTable(df,tableName)