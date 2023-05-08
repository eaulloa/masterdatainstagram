-- Databricks notebook source
CREATE TABLE profile_hashtags AS
(SELECT DISTINCT profile_id, explode(profile_hashtags) AS hashtag FROM instagram_data)