# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###dimuser autoloader and transformation
# MAGIC

# COMMAND ----------

df_dimuser=spark.read.format('parquet')\
    .load('/Volumes/spotify_project/schema/bronze/DimUser/')
display(df_dimuser)

# COMMAND ----------

def transform_dimuser(df):

    df_user = (df.withColumn("user_name", upper(col("user_name")))\
        .dropDuplicates(["user_id"])).drop('_rescued_data')
    return df_user


# COMMAND ----------

df_user = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/Volumes/spotify_project/schema/silver/DimUser/schema") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("/Volumes/spotify_project/schema/bronze/DimUser/")

df_user = transform_dimuser(df_user)

df_user.writeStream.format('delta')\
    .outputMode('append')\
    .option("checkpointLocation", "/Volumes/spotify_project/schema/silver/DimUser/checkpoint")\
    .option("mergeSchema", "true")\
    .trigger(once=True)\
            .toTable("spotify_project.schema.DimUser")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_project.schema.DimUser

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimArtist**

# COMMAND ----------

df=spark.read.format('parquet')\
    .load('/Volumes/spotify_project/schema/bronze/DimArtist/')
display(df)

# COMMAND ----------

def transform_dimArtist(df):

    df_user = (df.withColumn("artist_name", upper(col("artist_name")))\
        .dropDuplicates(["artist_id"])).drop('_rescued_data')
    return df_user

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation","/Volumes/spotify_project/schema/silver/DimArtist/schema")\
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
            .load("/Volumes/spotify_project/schema/bronze/DimArtist/")

df_art = transform_dimArtist(df_art)

df_art.writeStream.format('delta')\
    .outputMode('append')\
    .option("checkpointLocation", "/Volumes/spotify_project/schema/silver/DimArtist/checkpoint")\
    .option("mergeSchema", "true")\
    .trigger(once=True)\
            .toTable("spotify_project.schema.DimArtist")
        

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from spotify_project.schema.DimArtist

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimTrack**

# COMMAND ----------

df=spark.read.format('parquet')\
    .load('/Volumes/spotify_project/schema/bronze/DimTrack/')
display(df)

# COMMAND ----------

def transform_dimTrack(df):
    df = df.withColumn("durationFlag",when(col("duration_sec") < 150, "low")\
        .when(col("duration_sec") < 300, "medium")\
        .otherwise("high") )

    df = df.withColumn("track_name",regexp_replace(col("track_name"), "-", " "))
    df = df.dropDuplicates(["track_id"]).drop("_rescued_data")

    return df


# COMMAND ----------

df_track=spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','parquet')\
        .option('cloudfiles.schemaLocation','/Volumes/spotify_project/schema/silver/dimTrack/schema')\
        .option('cloudFiles.schemaEvolutionMode','addNewColumns')\
            .load('/Volumes/spotify_project/schema/bronze/DimTrack/')

df_track = transform_dimTrack(df_track)

df_track.writeStream.format('delta')\
    .outputMode('append')\
        .option('checkpointLocation','/Volumes/spotify_project/schema/silver/dimTrack/checkpoint')\
            .option('mergeSchema','true')\
                .trigger(once=True)\
                    .toTable('spotify_project.schema.DimTrack')




# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from spotify_project.schema.DimTrack

# COMMAND ----------

# MAGIC %md
# MAGIC ## DimDate

# COMMAND ----------

df_date=spark.read.format('parquet')\
    .load('/Volumes/spotify_project/schema/bronze/DimDate/')
display(df_date)


# COMMAND ----------

def transform_dimTrack(df):
    df=df.drop('_rescued_data')
    return df

# COMMAND ----------

df_date=spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','parquet')\
    .option('cloudFiles.schemaLocation','/Volumes/spotify_project/schema/silver/dimDate/schema')\
        .option('cloudFiles.schemaEvolutionMode','addNewColumns')\
            .load('/Volumes/spotify_project/schema/bronze/DimDate/')

df_date=transform_dimTrack(df_date)

df_date.writeStream.format('delta')\
    .outputMode('append')\
        .option('checkpointLocation','/Volumes/spotify_project/schema/silver/dimDate/checkpoint')\
            .option('mergeSchema','true')\
                .trigger(once=True)\
                    .toTable('spotify_project.schema.Dimdate')


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from spotify_project.schema.Dimdate

# COMMAND ----------

# MAGIC %md
# MAGIC ## **FactStream**

# COMMAND ----------

def transform_fact(df):
    df=df.drop('_rescued_data')
    return df

# COMMAND ----------

df_date=spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','parquet')\
    .option('cloudFiles.schemaLocation','/Volumes/spotify_project/schema/silver/factstream/schema')\
        .option('cloudFiles.schemaEvolutionMode','addNewColumns')\
            .load('/Volumes/spotify_project/schema/bronze/FactStream/')

df_date=transform_dimTrack(df_date)

df_date.writeStream.format('delta')\
    .outputMode('append')\
        .option('checkpointLocation','/Volumes/spotify_project/schema/silver/factstream/checkpoint')\
            .option('mergeSchema','true')\
                .trigger(once=True)\
                    .toTable('spotify_project.schema.factstream')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from spotify_project.schema.factstream

# COMMAND ----------



# COMMAND ----------

