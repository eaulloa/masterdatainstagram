-- Databricks notebook source
DROP TABLE IF EXISTS instagram_data;
CREATE TABLE instagram_data (
  post_id STRING,
  profile_id INT,
  location_id INT,
  post_type INT,
  description STRING,
  cts TIMESTAMP,
  number_likes INT,
  number_comments INT,
  post_type_desc STRING,
  hashtags ARRAY<STRING>,
  emojis ARRAY<STRING>,
  profile_name STRING,
  firstname_lastname STRING,
  Following INT,
  Followers INT,
  n_posts INT,
  url STRING,
  is_business_account BOOLEAN,
  profile_description STRING,
  profile_hashtags ARRAY<STRING>,
  profile_emojis ARRAY<STRING>,
  post_year INT,
  post_month INT,
  post_day INT,
  post_hour INT)
USING delta
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')
LOCATION 'abfss://refined@stmasterdatalake.dfs.core.windows.net/instagram_data'

-- COMMAND ----------

INSERT INTO instagram_data
SELECT * EXCEPT(ipr.profile_id, ipr.description, ipr.emojis, ipr.hashtags)
, ipr.description AS profile_description
, ipr.hashtags AS profile_hashtags
, ipr.emojis AS profile_emojis
, YEAR(ip.cts) as post_year
, CASE WHEN MONTH(ip.cts) < 10 then CONCAT(0,MONTH(ip.cts)) else MONTH(ip.cts) end as post_month
, CASE WHEN DAY(ip.cts) < 10 then CONCAT(0,DAY(ip.cts)) else DAY(ip.cts) end as post_day
, HOUR(cts) AS post_hour
FROM instagram_posts ip
INNER JOIN instagram_profiles ipr
ON ip.profile_id = ipr.profile_id