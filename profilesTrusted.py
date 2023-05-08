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
sourceTable = 'instagram_profiles_raw'
destinationTable = 'instagram_profiles' 

# COMMAND ----------

# MAGIC %run ../utils/functions

# COMMAND ----------

# MAGIC %md
# MAGIC ###Seleccionar campos, borrar duplicados por y filtrar nulos

# COMMAND ----------

instagramProfilesDf = (spark.read.table(sourceTable)
           .select('profile_id','profile_name','firstname_lastname','description','Following','Followers','n_posts','url','is_business_account')
           .filter('profile_id is not null and description is not null and profile_name is not null and firstname_lastname is not null')
           .dropDuplicates())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cambiar tipo de datos

# COMMAND ----------

instagramProfilesDf = (instagramProfilesDf           
            #.withColumn('profile_id',col('profile_id').cast(IntegerType()))
            .withColumn('Following',col('Following').cast(IntegerType()))
            .withColumn('Followers',col('Followers').cast(IntegerType()))
            .withColumn('n_posts',col('n_posts').cast(IntegerType()))
            .withColumn('is_business_account',col('is_business_account').cast(BooleanType())))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extraer Hashtags en un campo nuevo

# COMMAND ----------

instagramProfilesDf.createOrReplaceTempView('post_tempview')
instagramProfilesDf = spark.sql("""select *, regexp_extract_all(description, '(#\\\\w+)', 1) as hashtags from post_tempview where description like '%#%'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extraer emojis

# COMMAND ----------

import emoji

def filter_emojis(sentence):
    emojis = emoji.distinct_emoji_list(sentence)
    return emojis

extract_emojis = udf(filter_emojis,ArrayType(StringType()))

df = instagramProfilesDf.withColumn('emojis', extract_emojis(col('description')))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Quitar emojis del nombre

# COMMAND ----------

def remove_emojis(sentence):
    emojis = emoji.replace_emoji(sentence, replace='')
    return emojis

remove_emojis_udf = udf(remove_emojis)

df2 = df.withColumn('firstname_lastname', remove_emojis_udf(col('firstname_lastname')))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Escribir en tabla delta

# COMMAND ----------

(df2.write.format('delta')
   .option("mergeSchema", "true")
   .mode('overwrite')
   .option('path',trustedPath+destinationTable)
   .saveAsTable(destinationTable))