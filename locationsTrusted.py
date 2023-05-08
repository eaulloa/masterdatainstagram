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

landingPath = f'abfss://{sourceContainer}@{adlsAccountName}.dfs.core.windows.net/'
trustedPath = f'abfss://{destinationContainer}@{adlsAccountName}.dfs.core.windows.net/'
sourceTable = 'instagram_locations_raw'
destinationTable = 'instagram_locations' 

# COMMAND ----------

# MAGIC %run ../utils/functions

# COMMAND ----------

# MAGIC %md
# MAGIC ###Seleccionar campos, borrar duplicados por "id" y filtrar nulos en campo "name"

# COMMAND ----------

instagramLocationsDf = (spark.read.table(sourceTable)
           .select('id','name','street','zip','city','phone','lat','lng','cd')
           .filter('id is not null')
           .filter("id not like '%null%'")
           #.withColumn("id", when((col("id").isNull()) | (col("id").like("%null%")), monotonically_increasing_id()).otherwise(col("id")))
           .dropDuplicates(['id'])
           .filter('name is not null')
           .filter('cd is not null'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cambiar tipo de dato

# COMMAND ----------

instagramLocationsDf = (instagramLocationsDf           
            .withColumn('lat',col('lat').cast(DecimalType(10,5)))
            .withColumn('lng',col('lng').cast(DecimalType(10,5)))
            .withColumnRenamed('lat','lng_tmp')
            .withColumnRenamed('lng','lat')
            .withColumnRenamed('lng_tmp','lng'))

# COMMAND ----------

(instagramLocationsDf.write.format('delta')
   .option("mergeSchema", "true")
   .mode('overwrite')
   .option('path',trustedPath+destinationTable)
   .saveAsTable(destinationTable))