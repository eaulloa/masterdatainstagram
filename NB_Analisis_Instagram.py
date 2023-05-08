# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT ip.*  
# MAGIC FROM instagram_posts ip
# MAGIC INNER JOIN instagram_profiles ipr
# MAGIC ON ip.profile_id = ipr.profile_id
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from instagram_profiles
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC --buscar crear una columna de rangos de followers 
# MAGIC 
# MAGIC 
# MAGIC with A as (
# MAGIC SELECT *, 
# MAGIC CASE WHEN Followers <= 1000 THEN '<1K' 
# MAGIC WHEN  Followers > 1000 AND Followers <= 5000 THEN '1K-5K'
# MAGIC WHEN  Followers > 5000 AND Followers <= 15000 THEN '5K-15K'
# MAGIC WHEN  Followers > 15000 AND Followers <= 50000 THEN '15K-50K'
# MAGIC WHEN  Followers > 50000 AND Followers <= 100000 THEN '50K-100K'
# MAGIC WHEN  Followers > 100000  THEN '>100K'
# MAGIC END AS Followers_Range
# MAGIC FROM instagram_data ipr
# MAGIC )
# MAGIC select distinct profile_id 
# MAGIC from A 
# MAGIC where Followers_Range = '>100K'

# COMMAND ----------

# MAGIC  %sql
# MAGIC 
# MAGIC select 'instagram_posts', count(*) from instagram_posts as count
# MAGIC union all
# MAGIC select 'instagram_locations', count(*) from instagram_locations as count
# MAGIC union all
# MAGIC select 'instagram_profiles', count(*) from instagram_profiles as count

# COMMAND ----------

# MAGIC %sql
# MAGIC --join de instagram_data y locations (sale al rededor de 3K resultados muy poco , no hacen match los id de locations con los posts)
# MAGIC SELECT * EXCEPT(ipr.profile_id, ipr.description, ipr.emojis, ipr.hashtags, il.id), ipr.description AS profile_description, ipr.hashtags AS profile_hashtags, ipr.emojis AS profile_emojis
# MAGIC FROM instagram_posts ip
# MAGIC INNER JOIN instagram_profiles ipr
# MAGIC ON ip.profile_id = ipr.profile_id
# MAGIC LEFT JOIN instagram_locations il
# MAGIC ON ip.location_id = il.id

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, concat(year,month,day) as YYYYMMDD from (
# MAGIC select cts
# MAGIC , year(cts) as year
# MAGIC , case when month(cts) < 10 then concat(0,month(cts)) else month(cts) end as month
# MAGIC , case when day(cts) < 10 then concat(0,day(cts)) else day(cts) end as day
# MAGIC , hour(cts) 
# MAGIC from instagram_posts limit 100
# MAGIC )t1