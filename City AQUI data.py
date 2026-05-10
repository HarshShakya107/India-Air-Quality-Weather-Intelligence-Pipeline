# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

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

import requests
from pyspark.sql import SparkSession
from datetime import datetime
spark = SparkSession.builder.appName("AirQualityFetch").getOrCreate()

capitals = [
    ("Andhra Pradesh",    "Amaravati",          16.5176,  80.5179),
    ("Arunachal Pradesh", "Itanagar",            27.0844,  93.6053),
    ("Assam",             "Dispur",              26.1445,  91.7362),
    ("Bihar",             "Patna",               25.5941,  85.1376),
    ("Chhattisgarh",      "Raipur",              21.2514,  81.6296),
    ("Goa",               "Panaji",              15.4989,  73.8278),
    ("Gujarat",           "Gandhinagar",         23.2156,  72.6369),
    ("Haryana",           "Chandigarh",          30.7333,  76.7794),
    ("Himachal Pradesh",  "Shimla",              31.1048,  77.1734),
    ("Jharkhand",         "Ranchi",              23.3441,  85.3096),
    ("Karnataka",         "Bengaluru",           12.9716,  77.5946),
    ("Kerala",            "Thiruvananthapuram",   8.5241,  76.9366),
    ("Madhya Pradesh",    "Bhopal",              23.2599,  77.4126),
    ("Maharashtra",       "Mumbai",              19.0760,  72.8777),
    ("Manipur",           "Imphal",              24.8170,  93.9368),
    ("Meghalaya",         "Shillong",            25.5788,  91.8933),
    ("Mizoram",           "Aizawl",              23.7307,  92.7173),
    ("Nagaland",          "Kohima",              25.6701,  94.1077),
    ("Odisha",            "Bhubaneswar",         20.2961,  85.8245),
    ("Punjab",            "Chandigarh",          30.7333,  76.7794),
    ("Rajasthan",         "Jaipur",              26.9124,  75.7873),
    ("Sikkim",            "Gangtok",             27.3389,  88.6065),
    ("Tamil Nadu",        "Chennai",             13.0827,  80.2707),
    ("Telangana",         "Hyderabad",           17.3850,  78.4867),
    ("Tripura",           "Agartala",            23.8315,  91.2868),
    ("Uttar Pradesh",     "Lucknow",             26.8467,  80.9462),
    ("Uttarakhand",       "Dehradun",            30.3165,  78.0322),
    ("West Bengal",       "Kolkata",             22.5726,  88.3639),
    ("Delhi (UT)",        "New Delhi",           28.6139,  77.2090),
    ("J&K (UT)",          "Srinagar",            34.0837,  74.7973),
    ("Ladakh (UT)",       "Leh",                 34.1526,  77.5771),
    ("Puducherry (UT)",   "Puducherry",          11.9416,  79.8083),
]


START = datetime.now().strftime("%Y-%m-%d")
END   = datetime.now().strftime("%Y-%m-%d")

def fetch_aqi(state, city, lat, lon, start_date, end_date):
    url = (
        f"https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={lat}&longitude={lon}"
        f"&hourly=pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,european_aqi"
        f"&start_date={start_date}&end_date={end_date}"
    )

    try:
        res  = requests.get(url, timeout=10)
        data = res.json()

        if "hourly" not in data:
            print(f"No data for {city}: {data}")
            return []

        h    = data["hourly"]
        rows = []

        for i in range(len(h["time"])):
            rows.append({
                "state":     state,
                "capital":   city,
                "latitude":  lat,
                "longitude": lon,
                "timestamp": h["time"][i],
                "aqi":       h["european_aqi"][i],
                "pm25":      h["pm2_5"][i],
                "pm10":      h["pm10"][i],
                "co":        h["carbon_monoxide"][i],
                "no2":       h["nitrogen_dioxide"][i],
                "so2":       h["sulphur_dioxide"][i],
                "o3":        h["ozone"][i],
            })
        return rows

    except Exception as e:
        print(f"Error fetching {city}: {e}")
        return []

all_rows = []
for state, city, lat, lon in capitals:
    rows = fetch_aqi(state, city, lat, lon, START, END)
    all_rows.extend(rows)
    print(f"Fetched: {city} — {len(rows)} rows")

df1 = spark.createDataFrame(all_rows)
df1.display()


# COMMAND ----------

df1.write\
    .mode("append")\
    .format("delta")\
    .save("abfss://bronze@environmentharsh.dfs.core.windows.net/bronze/air_quality")

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from datetime import datetime
spark = SparkSession.builder.appName("WeatherFetch").getOrCreate()

capitals = [
    ("Andhra Pradesh",    "Amaravati",          16.5176,  80.5179),
    ("Arunachal Pradesh", "Itanagar",            27.0844,  93.6053),
    ("Assam",             "Dispur",              26.1445,  91.7362),
    ("Bihar",             "Patna",               25.5941,  85.1376),
    ("Chhattisgarh",      "Raipur",              21.2514,  81.6296),
    ("Goa",               "Panaji",              15.4989,  73.8278),
    ("Gujarat",           "Gandhinagar",         23.2156,  72.6369),
    ("Haryana",           "Chandigarh",          30.7333,  76.7794),
    ("Himachal Pradesh",  "Shimla",              31.1048,  77.1734),
    ("Jharkhand",         "Ranchi",              23.3441,  85.3096),
    ("Karnataka",         "Bengaluru",           12.9716,  77.5946),
    ("Kerala",            "Thiruvananthapuram",   8.5241,  76.9366),
    ("Madhya Pradesh",    "Bhopal",              23.2599,  77.4126),
    ("Maharashtra",       "Mumbai",              19.0760,  72.8777),
    ("Manipur",           "Imphal",              24.8170,  93.9368),
    ("Meghalaya",         "Shillong",            25.5788,  91.8933),
    ("Mizoram",           "Aizawl",              23.7307,  92.7173),
    ("Nagaland",          "Kohima",              25.6701,  94.1077),
    ("Odisha",            "Bhubaneswar",         20.2961,  85.8245),
    ("Punjab",            "Chandigarh",          30.7333,  76.7794),
    ("Rajasthan",         "Jaipur",              26.9124,  75.7873),
    ("Sikkim",            "Gangtok",             27.3389,  88.6065),
    ("Tamil Nadu",        "Chennai",             13.0827,  80.2707),
    ("Telangana",         "Hyderabad",           17.3850,  78.4867),
    ("Tripura",           "Agartala",            23.8315,  91.2868),
    ("Uttar Pradesh",     "Lucknow",             26.8467,  80.9462),
    ("Uttarakhand",       "Dehradun",            30.3165,  78.0322),
    ("West Bengal",       "Kolkata",             22.5726,  88.3639),
    ("Delhi (UT)",        "New Delhi",           28.6139,  77.2090),
    ("J&K (UT)",          "Srinagar",            34.0837,  74.7973),
    ("Ladakh (UT)",       "Leh",                 34.1526,  77.5771),
    ("Puducherry (UT)",   "Puducherry",          11.9416,  79.8083),
]

def fetch_weather(state, city, lat, lon, start_date, end_date):
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,temperature_2m_min,wind_speed_10m_max,precipitation_sum"
        f"&start_date={start_date}&end_date={end_date}"
    )
    try:
        res = requests.get(url, timeout=10)
        daily = res.json()["daily"]

        rows = []
        for i in range(len(daily["time"])):
            rows.append({
                "state":            state,
                "capital":          city,
                "latitude":         lat,
                "longitude":        lon,
                "date":             daily["time"][i],
                "max_temp_c":       daily["temperature_2m_max"][i],
                "min_temp_c":       daily["temperature_2m_min"][i],
                "wind_speed_kmph":  daily["wind_speed_10m_max"][i],
                "precipitation_mm": daily["precipitation_sum"][i],
            })
        return rows

    except Exception as e:
        print(f"Error fetching {city}: {e}")
        return []

START = datetime.now().strftime("%Y-%m-%d")
END   = datetime.now().strftime("%Y-%m-%d")

all_rows = []
for state, city, lat, lon in capitals:
    rows = fetch_weather(state, city, lat, lon, START, END)
    all_rows.extend(rows)
    print(f"Fetched: {city} — {len(rows)} rows")

df2 = spark.createDataFrame(all_rows)
df2.display()

# COMMAND ----------

df2.write\
    .mode("append")\
    .format("delta")\
    .save("abfss://bronze@environmentharsh.dfs.core.windows.net/bronze/weather")