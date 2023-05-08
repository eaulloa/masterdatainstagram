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
sourceTable = 'instagram_posts_raw'
destinationTable = 'instagram_posts' 

# COMMAND ----------

# MAGIC %run ../utils/functions

# COMMAND ----------

# MAGIC %md
# MAGIC ###Seleccionar campos, borrar duplicados por y filtrar nulos

# COMMAND ----------

instagramPostsDf = (spark.read.table(sourceTable)
           .select('post_id','profile_id','location_id','post_type','description','cts','numbr_likes','number_comments')
           .filter('post_id is not null and profile_id is not null and post_type is not null and number_comments is not null and numbr_likes is not null and description is not null and cts is not null')
           .dropDuplicates())

# COMMAND ----------

instagramPostsDf = (instagramPostsDf                       
            .withColumn('profile_id',col('profile_id').cast(IntegerType()))
            .withColumn('location_id',col('location_id').cast(IntegerType()))
            .withColumn('post_type',col('post_type').cast(IntegerType()))        
            .withColumn('numbr_likes',col('numbr_likes').cast(IntegerType()))
            .withColumn('number_comments',col('number_comments').cast(IntegerType()))
            .withColumnRenamed('numbr_likes','number_likes')
            .withColumn('cts',col('cts').cast(TimestampType()))
            .withColumn('post_type_desc', when(col('post_type') == '1','Photo').when(col('post_type')==2, 'Video').otherwise('Contenido multiple')))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extraer Hashtags en un campo nuevo

# COMMAND ----------

instagramPostsDf.createOrReplaceTempView('post_tempview')
instagramPostsFieldsDf = spark.sql("""select *, regexp_extract_all(description, '(#\\\\w+)', 1) as hashtags from post_tempview where description like '%#%'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extraer emojis

# COMMAND ----------

import emoji

def filter_emojis(sentence):
    emojis = emoji.distinct_emoji_list(sentence)
    return emojis

extract_emojis = udf(filter_emojis,ArrayType(StringType()))

df = instagramPostsFieldsDf.withColumn('emojis', extract_emojis(col('description')))

# COMMAND ----------

(df.write.format('delta')
   .mode('overwrite')
   .option('path',trustedPath+destinationTable)
   .saveAsTable(destinationTable))