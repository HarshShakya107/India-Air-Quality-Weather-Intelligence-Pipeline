# Databricks notebook source
service_credential = dbutils.secrets.get(scope="environment_scope", key="clientid")
app_id = dbutils.secrets.get(scope="environment_scope", key="Appid")
tenant_id = dbutils.secrets.get(scope="environment_scope", key="tenantid")

spark.conf.set(f"fs.azure.account.auth.type.environmentharsh.dfs.core.windows.net", "OAuth")

spark.conf.set(f"fs.azure.account.oauth.provider.type.environmentharsh.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set(f"fs.azure.account.oauth2.client.id.environmentharsh.dfs.core.windows.net",
               app_id)

spark.conf.set(f"fs.azure.account.oauth2.client.secret.environmentharsh.dfs.core.windows.net",
               service_credential)

spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.environmentharsh.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

weather=spark.readStream.format("delta").option("startingVersion","0").load("abfss://bronze@environmentharsh.dfs.core.windows.net/bronze/weather")
weather.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

weather=weather.withColumn("date",to_date(col("date"),"yyyy-MM-dd"))\
    .withColumn("year",year(col("date")))\
        .withColumn("month",month(col("date")))\
            .withColumn("day",dayofmonth(col("date")))\
                .withColumn("timestamp",to_timestamp(col("date"),"yyyy-MM-dd'T'HH:mm:ss"))

# COMMAND ----------

weather.printSchema()

# COMMAND ----------

query1=weather.writeStream\
    .format("delta")\
        .outputMode("append")\
            .trigger(availableNow=True)\
                .option("checkpointLocation","abfss://silver@environmentharsh.dfs.core.windows.net/checkpoint/weather")\
                    .start("abfss://silver@environmentharsh.dfs.core.windows.net/silver/weather")       
query1.awaitTermination()     


# COMMAND ----------

airquality=spark.readStream.format("delta").option("startingVersion","0").load("abfss://bronze@environmentharsh.dfs.core.windows.net/bronze/air_quality")
airquality.printSchema()
airquality=airquality.withColumn("timestamp",to_timestamp(col("timestamp"),"yyyy-MM-dd'T'HH:mm"))
airquality.printSchema()

# COMMAND ----------

query2=airquality.writeStream\
    .format("delta")\
        .outputMode("append")\
            .trigger(availableNow=True)\
                .option("checkpointLocation","abfss://silver@environmentharsh.dfs.core.windows.net/checkpoint/air_quality")\
                    .start("abfss://silver@environmentharsh.dfs.core.windows.net/silver/air_quality")
query2.awaitTermination()


# COMMAND ----------

weather_data=spark.readStream.format("delta").option("startingVersion","0").load("abfss://silver@environmentharsh.dfs.core.windows.net/silver/weather")
airquality_data=spark.readStream.format("delta").option("startingVersion","0").load("abfss://silver@environmentharsh.dfs.core.windows.net/silver/air_quality")

weather_streaming=weather_data.withWatermark("timestamp","10 minutes")
airquality_streaming=airquality_data.withWatermark("timestamp","10 minutes")

# COMMAND ----------

environment = airquality_streaming.join(
    weather_streaming,
    (airquality_streaming["capital"] == weather_streaming["capital"]) &
    (airquality_streaming["state"] == weather_streaming["state"]) &
    (airquality_streaming["timestamp"] >= weather_streaming["timestamp"]) &
    (airquality_streaming["timestamp"] <= weather_streaming["timestamp"] + expr("INTERVAL 1 DAY")),
    "left"
).select(
    airquality_streaming["capital"],
    airquality_streaming["state"],
    weather_streaming["date"],
    weather_streaming["year"],
    weather_streaming["month"],
    weather_streaming["day"],
    airquality_streaming["timestamp"],
    airquality_streaming["latitude"],
    airquality_streaming["longitude"],
    airquality_streaming["aqi"],
    airquality_streaming["co"],
    airquality_streaming["no2"],
    airquality_streaming["o3"],
    airquality_streaming["pm10"],
    airquality_streaming["pm25"],
    airquality_streaming["so2"],
    weather_streaming["max_temp_c"],
    weather_streaming["min_temp_c"],
    weather_streaming["precipitation_mm"],
    weather_streaming["wind_speed_kmph"]
)

# COMMAND ----------

environment.printSchema()

# COMMAND ----------

environment=environment.withColumn("diseases",when(col("aqi")<=100,"'Mild cough','Throat irritation',Reduced excercise capacity','Mild Mild cardiovascular stress in sensitive groups','Eye redness','Mild skin dryness").when(col("aqi")<=150,"'Asthma attacks','Bronchitis','Allergic rhinitis','Sinusitis','Bronchoconstriction','Arrhythmia','Increased BP','Angina in heart patients','Increased susceptibility to respitatory infections','Viral pneumonia','Conjunctivitis','Eczema worsening','Contact dermatitis'").when(col("aqi")<=200,"'Pulmonary edema','COPD exacerbation','Chronic bronchitis','Reduced lung function (permanent risk)','Myocardial stress','Atherosclerosis progression','Blood clot risk','Ischemic heart disease','Cognitive decline (long-term)','Headache','Dizziness','Dementia risk','Reduced fertility','Preterm birth risk','Low birth weight','Diabetes risk increase','Inflammation markers elevated'").when(col("aqi")<=300,"'Respiratory failure','Pulmonary hemorrhage','Severe chemical pneumonitis','Pulmonary fibrosis (long-term)','Heart failure','Stroke','Cardiac arrest','Severe arrhythmia','Severe arrhythmia','Confusion','Memory loss','Brain damage','Fetal brain development harm','Miscarriage risk','Birth defects','Lung cancer (with prolonged exposure)','Bladder cancer links (NO2)'").when(col("aqi")>=300,"'Respiratory arrest','Irreversible alveolar destruction','Suffocation','Multi-organ ischemia','Fatal cardiac events','Disseminated intravascular coagulation','Coma','Permanent neurological damage','Seizures','Multi-organ failure','Sepsis-like systemic inflammation','Death'").otherwise("No diseases"))\
    .withColumn("danger_level",when(col("aqi")<=100,"Moderate").when(col("aqi")<=150,"Unhealthy for Sensitive ").when(col("aqi")<=200,"Unhealthy").when(col("aqi")<=300,"Very Unhealthy").when(col("aqi")>=300,"Hazardous").otherwise("Safe"))

# COMMAND ----------

environment=environment.withColumn("risk_score",((col("aqi")/500)*0.35+(col("pm25")/300)*0.25 +(col("pm10")/500)*0.15 + (col("co")/2000)*0.15 + (col("no2")/400)*0.10) * 100)\
    .withColumn("risk_score",round(col("risk_score"),2))\
        .withColumn("risk_level",when(col("risk_score") < 20, "Low")
        .when(col("risk_score") < 40, "Moderate")
        .when(col("risk_score") < 60, "High")
        .when(col("risk_score") < 80, "Very High")
        .otherwise("Critical")
)
environment=environment.withColumn("recommendation",when(col("aqi")<50,"Safe").when(col("aqi")<100,"Moderate").when(col("aqi")<200,"Avoid prolonged outdoor exposure").otherwise("Stay indore"))

# COMMAND ----------

environment=environment.withColumn("geo_risk_zone",when(col("aqi")<50,"Safe").when(col("aqi")<100,"Yellow").when(col("aqi")<200,"Orange").otherwise("Red"))

# COMMAND ----------

query3=environment.writeStream\
    .format("delta")\
        .outputMode("append")\
            .trigger(availableNow=True)\
                .option("checkpointLocation","abfss://silver@environmentharsh.dfs.core.windows.net/checkpoint/environment_stream")\
                    .start("abfss://silver@environmentharsh.dfs.core.windows.net/silver/environment_stream")

query3.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Cell 17
environment_data_table=spark.readStream.format("delta").option("startingVersion","0").load("abfss://silver@environmentharsh.dfs.core.windows.net/silver/environment_stream")

query4=environment_data_table.writeStream\
    .format("delta")\
        .outputMode("append")\
            .trigger(availableNow=True)\
                .option("checkpointLocation","abfss://gold@environmentharsh.dfs.core.windows.net/checkpoint/environment_stream")\
                    .start("abfss://gold@environmentharsh.dfs.core.windows.net/gold/environment_stream")
query4.awaitTermination()
            

# COMMAND ----------

from pyspark.sql.window import *
location_dim=environment_data_table.select("state","capital","latitude","longitude").distinct()
loaction_dim=location_dim.withColumn("location_id",sha2(concat_ws("||",col("state"),col("capital")),256))


# COMMAND ----------

query5=location_dim.writeStream \
    .format("delta")\
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation","abfss://gold@environmentharsh.dfs.core.windows.net/checkpoint/location_dim")\
        .start("abfss://gold@environmentharsh.dfs.core.windows.net/gold/location_dim")
query5.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Cell 23
fact_environment=environment_data_table.select("state","capital","max_temp_c","min_temp_c","precipitation_mm","wind_speed_kmph","aqi","co","no2","o3","pm10","pm25","so2","danger_level","diseases").distinct()
fact_environment=fact_environment.withColumn("fact_id",sha2(concat_ws("||",col("state"),col("capital")),256)).drop("state","capital")


# COMMAND ----------

query6=fact_environment.writeStream\
    .format("delta")\
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation","abfss://gold@environmentharsh.dfs.core.windows.net/checkpoint/fact_envrionment")\
        .start("abfss://gold@environmentharsh.dfs.core.windows.net/gold/fact_envrionment")
query6.awaitTermination()

# COMMAND ----------

date_dim=environment_data_table.select("state","capital","date","timestamp").distinct()
date_dim=date_dim.withColumn("date_id",sha2(concat_ws("||",col("state"),col("capital")),256)).drop("state","capital")


# COMMAND ----------

# DBTITLE 1,Cell 22
query7=date_dim.writeStream\
    .format("delta")\
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation","abfss://gold@environmentharsh.dfs.core.windows.net/checkpoint/date_dim")\
        .start("abfss://gold@environmentharsh.dfs.core.windows.net/gold/date_dim")

query7.awaitTermination()


# COMMAND ----------

environment_data_table_df=spark.read.format("delta").load("abfss://gold@environmentharsh.dfs.core.windows.net/gold/environment_stream")


# COMMAND ----------

environment_report = environment_data_table_df.groupBy(
    "capital",
    "year",
    "month"
).agg(
    avg("aqi").alias("average_aqi"),
    max("aqi").alias("max_aqi"),
    min("aqi").alias("min_aqi"),
    avg("co").alias("avg_co"),
    max("co").alias("max_co"),
    min("co").alias("min_co"),
    avg("no2").alias("avg_no2"),
    max("no2").alias("max_no2"),
    min("no2").alias("min_no2"),
    avg("o3").alias("avg_o3"),
    max("o3").alias("max_o3"),
    min("o3").alias("min_o3"),
    avg("pm10").alias("avg_pm10"),
    max("pm10").alias("max_pm10"),
    min("pm10").alias("min_pm10"),
    avg("pm25").alias("avg_pm25"),
    max("pm25").alias("max_pm25"),
    min("pm25").alias("min_pm25"),
    avg("so2").alias("avg_so2"),
    max("so2").alias("max_so2"),
    min("so2").alias("min_so2"),
    avg("max_temp_c").alias("avg_max_temp_c"),
    max("max_temp_c").alias("max_max_temp_c"),
    min("max_temp_c").alias("min_max_temp_c"),
    avg("min_temp_c").alias("avg_min_temp_c"),
    max("min_temp_c").alias("max_min_temp_c"),
    min("min_temp_c").alias("min_min_temp_c"),
    avg("precipitation_mm").alias("avg_precipitation_mm"),
    max("precipitation_mm").alias("max_precipitation_mm"),
    min("precipitation_mm").alias("min_precipitation_mm"),
    avg("wind_speed_kmph").alias("avg_wind_speed_kmph"),
    max("wind_speed_kmph").alias("max_wind_speed_kmph"),
    min("wind_speed_kmph").alias("min_wind_speed_kmph"),
    mode(col("danger_level")).alias("common_danger_level"),
    avg("risk_score").alias("avg_risk_score"),
    mode(col("risk_level")).alias("common_risk_level"),
    mode(col("recomandation")).alias("common_recommendation"),
    mode(col("geo_risk_zone")).alias("common_geo_risk_zone")
)

# COMMAND ----------

environment_report.display()

# COMMAND ----------

environment_report.write\
    .format("delta")\
    .mode("append")\
    .save("abfss://gold@environmentharsh.dfs.core.windows.net/gold/environment_report")


# COMMAND ----------

environment_report.write\
    .format("delta")\
        .mode("append")\
        .saveAsTable("bicatalog.gold.environment_report")

# COMMAND ----------

from pyspark.sql.window import *
window_spec = Window.partitionBy("capital") \
    .orderBy("year", "month", "day") \
    .rowsBetween(-6, 0)
environment_report_rolling_data = environment_data_table_df.select(
    "capital",
    "year",
    "month",
    "day",
    avg("aqi").over(window_spec).alias("average_rolling_aqi"),
    max("aqi").over(window_spec).alias("max_aqi"),
    min("aqi").over(window_spec).alias("min_aqi"),

    avg("co").over(window_spec).alias("avg_co"),
    max("co").over(window_spec).alias("max_co"),
    min("co").over(window_spec).alias("min_co"),

    avg("no2").over(window_spec).alias("avg_no2"),
    max("no2").over(window_spec).alias("max_no2"),
    min("no2").over(window_spec).alias("min_no2"),

    avg("o3").over(window_spec).alias("avg_o3"),
    max("o3").over(window_spec).alias("max_o3"),
    min("o3").over(window_spec).alias("min_o3"),

    avg("pm10").over(window_spec).alias("avg_pm10"),
    max("pm10").over(window_spec).alias("max_pm10"),
    min("pm10").over(window_spec).alias("min_pm10"),

    avg("pm25").over(window_spec).alias("avg_pm25"),
    max("pm25").over(window_spec).alias("max_pm25"),
    min("pm25").over(window_spec).alias("min_pm25"),

    avg("so2").over(window_spec).alias("avg_so2"),
    max("so2").over(window_spec).alias("max_so2"),
    min("so2").over(window_spec).alias("min_so2"),

    avg("max_temp_c").over(window_spec).alias("avg_max_temp_c"),
    max("max_temp_c").over(window_spec).alias("max_max_temp_c"),
    min("max_temp_c").over(window_spec).alias("min_max_temp_c"),

    avg("min_temp_c").over(window_spec).alias("avg_min_temp_c"),
    max("min_temp_c").over(window_spec).alias("max_min_temp_c"),
    min("min_temp_c").over(window_spec).alias("min_min_temp_c"),

    avg("precipitation_mm").over(window_spec).alias("avg_precipitation_mm"),
    max("precipitation_mm").over(window_spec).alias("max_precipitation_mm"),
    min("precipitation_mm").over(window_spec).alias("min_precipitation_mm"),

    avg("wind_speed_kmph").over(window_spec).alias("avg_wind_speed_kmph"),
    max("wind_speed_kmph").over(window_spec).alias("max_wind_speed_kmph"),
    min("wind_speed_kmph").over(window_spec).alias("min_wind_speed_kmph"),

    avg("risk_score").over(window_spec).alias("avg_risk_score")
)


# COMMAND ----------

environment_report_rolling_data.display()

# COMMAND ----------

environment_report_rolling_data.write\
    .format("delta")\
    .mode("append")\
    .save("abfss://gold@environmentharsh.dfs.core.windows.net/gold/environment_report_rolling_data")

# COMMAND ----------

environment_report_rolling_data.write\
    .format("delta")\
        .mode("append")\
        .saveAsTable("bicatalog.gold.environment_report_rolling_data")

# COMMAND ----------

environment_alert = environment_data_table_df.filter(
    (col("aqi") > 300) |
    (col("risk_score") > 60)
)

# COMMAND ----------

environment_alert.display()

# COMMAND ----------

environment_alert.write\
    .format("delta")\
    .mode("append")\
    .save("abfss://gold@environmentharsh.dfs.core.windows.net/gold/environment_alert")

# COMMAND ----------

environment_alert.write\
    .format("delta")\
        .mode("append")\
        .saveAsTable("bicatalog.gold.environment_alert")
