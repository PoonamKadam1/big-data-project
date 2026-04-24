"""
PROJECT: Scalable Big Data Platform for Climate-Health Analytics
AUTHOR: Poonam Vikram Kadam
COURSE: T-764 BIG DATA MANAGEMENT, Reykjavik University
DATE: April 2026

DESCRIPTION:
This script implements the Batch Layer of the proposed Big Data platform.
It integrates heterogeneous data (JSON/CSV) using Apache Spark,
performs K-Means clustering, and generates interactive dashboards.
The architecture follows the Lambda pattern (batch layer implemented;
speed layer designed as future extension with Kafka).
"""

import pandas as pd
import numpy as np
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg as spark_avg, first, create_map, split
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import itertools

# ============================================================
# 1. SPARK SESSION INITIALIZATION (PROCESSING LAYER)
# ============================================================
# In production, this runs on a cluster with HDFS storage.
# For prototype, local mode is used to demonstrate the logic.
# Apache Spark is selected for its in-memory processing, providing a 10-100x 
# speedup over MapReduce for iterative tasks like K-Means.
# Master("local[*]") enables horizontal scaling across all available cores.
spark = SparkSession.builder.appName("ClimateHealth_Final").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 2. LAMBDA ARCHITECTURE – SPEED LAYER (STREAMING MOCK)
# ============================================================
# The Lambda Architecture combines batch (this script) and real-time (Kafka+Spark Streaming).
# Below is a commented implementation of the speed layer using Kafka.
# In production, uncomment and configure with actual Kafka brokers.
"""
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

stream_schema = StructType([
    StructField("country", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("aqi", DoubleType()),
    StructField("co2", DoubleType())
])

stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "climate-sensors") \
    .load() \
    .select(from_json(col("value").cast("string"), stream_schema).alias("data")) \
    .select("data.*")

# Real-time alerting when AQI exceeds threshold
alert_df = stream_df.groupBy("country", window("timestamp", "5 minutes")) \
    .avg("aqi") \
    .filter("avg(aqi) > 150")

query = alert_df.writeStream.outputMode("update").format("console").start()
# query.awaitTermination()
print("Streaming pipeline ready (mock).")
"""

print("=" * 60)
print("CLIMATE-HEALTH BIG DATA PLATFORM")
print("ARCHITECTURE: Batch Layer (Spark) + HDFS-ready")
print("=" * 60)

# ============================================================
# 3. DATA INGESTION LAYER – VARIETY (JSON + CSV)
# ============================================================
# The system ingests climate data (JSON), PM2.5 data (CSV), and CO2 data (CSV).
# In production, these would be read from HDFS or cloud storage.
print("\n[Ingestion] Loading climate data from JSON...")
with open('main_climate_data.json', 'r') as f:
    climate_json = json.load(f)

# Parse nested JSON into flat DataFrame
climate_list = []
for code, months in climate_json['data']['tas'].items():
    for period, temp in months.items():
        try:
            y, m = map(int, period.split('-'))
            if 2000 <= y <= 2025:
                climate_list.append((code, y, m, float(temp) if temp else 0.0))
        except:
            continue
climate_spark_df = spark.createDataFrame(climate_list, ["Country_Code", "Year", "Month", "Mean_Temp_C"])

print("[Ingestion] Loading PM2.5 data from CSV...")
pm25_df = spark.read.csv("main_pm25_data.csv", header=True, inferSchema=True)

# ============================================================
# 4. DATA TRANSFORMATION & CLEANING (VERACITY)
# ============================================================
# Handle missing values, standardize country names, and aggregate monthly AQI.
print("[Cleaning] Mapping country codes...")
country_name_map = {
    "USA": "United States", "CAN": "Canada", "MEX": "Mexico",
    "COL": "Colombia", "ARG": "Argentina", "BRA": "Brazil", "PER": "Peru", "CHL": "Chile",
    "GBR": "United Kingdom", "DEU": "Germany", "FRA": "France", "ITA": "Italy",
    "ESP": "Spain", "NLD": "Netherlands", "SWE": "Sweden", "NOR": "Norway",
    "ISL": "Iceland", "IND": "India", "JPN": "Japan", "KOR": "South Korea",
    "CHN": "China", "AUS": "Australia", "NZL": "New Zealand", "ZAF": "South Africa"
}
mapping_pairs = [lit(k) for k in itertools.chain(*country_name_map.items())]
mapping_expr = create_map(mapping_pairs)

# The dataset may contain suffixes (e.g., _X0) from data preparation; strip them.
climate_spark_df = climate_spark_df.withColumn("Base_Code", split(col("Country_Code"), "_").getItem(0)) \
                                   .withColumn("Country_Name", mapping_expr[col("Base_Code")])

# Aggregate PM2.5 to monthly level and clean country names
pm25_aggregated = pm25_df.select(
    col("country_name").alias("Country"), col("year").alias("Year"),
    col("month").alias("Month"), col("air_quality_index").alias("AQI"), col("region").alias("Region")
).groupBy("Country", "Year", "Month").agg(
    spark_avg("AQI").alias("AQI"), first("Region").alias("Region")
).withColumn("Country", split(col("Country"), "_").getItem(0))

# ============================================================
# 5. JOIN OPERATIONS (INTEGRATION LAYER)
# ============================================================
# Join climate, PM2.5, and CO2 data on country, year, month.
print("[Integration] Joining datasets...")
joined_df = climate_spark_df.alias("climate").join(
    pm25_aggregated.alias("pm25"),
    (col("climate.Country_Name") == col("pm25.Country")) &
    (col("climate.Year") == col("pm25.Year")) &
    (col("climate.Month") == col("pm25.Month")), "left"
)

final_df = joined_df.select(
    col("climate.Country_Name").alias("Country"), col("climate.Year").alias("Year"),
    col("climate.Month").alias("Month"), col("climate.Mean_Temp_C").alias("Mean_Temp_C"),
    col("pm25.AQI").alias("AQI"), col("pm25.Region").alias("Region")
)

# Load CO2 emissions (another CSV source)
co2_df = spark.read.csv("co2_emissions.csv", header=True, inferSchema=True)
final_df = final_df.join(co2_df, on=["Country", "Year"], how="left")

# Derive health risk indicator (domain knowledge: 30% AQI + 20% temperature)
final_df = final_df.withColumn("Health_Risk", 0.3 * col("AQI") + 0.2 * col("Mean_Temp_C"))

# ============================================================
# 6. ANALYTICS: K-MEANS CLUSTERING (UNSUPERVISED LEARNING)
# ============================================================
# K-Means groups countries by climate-health profile.
# Features: Mean Temperature, AQI, Health Risk.
# K=3 chosen to identify low/medium/high risk groups.
print("[Analytics] Running K-Means clustering (k=3)...")
assembler = VectorAssembler(inputCols=["Mean_Temp_C", "AQI", "Health_Risk"], outputCol="features")
vector_df = assembler.transform(final_df.fillna(0))
kmeans = KMeans(k=3, seed=42)  # k=3 clusters, fixed seed for reproducibility
model = kmeans.fit(vector_df)
clustered_df = model.transform(vector_df)  # Adds 'prediction' column

# In production, the clustered data would be written to Cassandra for low-latency serving.
# final_df.write.format("org.apache.spark.sql.cassandra").options(...).save()

# ============================================================
# 7. SERVING LAYER: INTERACTIVE DASHBOARDS (PLOTLY)
# This layer derives "Value" by translating 2,851,200 rows of raw data into 
# visual insights. Plotly is used to showcase the final "Serving Layer" 
# logic, which in production would be powered by Apache Cassandra.
# ============================================================
# Prepare aggregated data for visualizations
print("[Serving] Building dashboards...")
monthly_stats = final_df.groupBy("Year", "Month").agg(
    spark_avg("Mean_Temp_C").alias("Temp"), spark_avg("AQI").alias("AQI"),
    spark_avg("Health_Risk").alias("Health"), spark_avg("CO2_Emissions").alias("CO2")
).orderBy("Year", "Month").toPandas()
monthly_stats['Date'] = monthly_stats['Year'].astype(str) + "-" + monthly_stats['Month'].astype(str).str.zfill(2)

regional_pd = final_df.groupBy("Region").agg(
    spark_avg("AQI").alias("AQI"), spark_avg("Health_Risk").alias("Health")
).toPandas()

map_pd = final_df.groupBy("Country", "Year").agg(
    spark_avg("Mean_Temp_C").alias("Temp"),
    spark_avg("AQI").alias("AQI"),
    spark_avg("Health_Risk").alias("Health")
).toPandas()

row_count = final_df.count()
print(f"[Volume] Total rows processed: {row_count:,}")

# ---- Global Dashboard (3 subplots: time series, regional bars, choropleth map) ----
fig = make_subplots(
    rows=3, cols=1, row_heights=[0.35, 0.30, 0.35], vertical_spacing=0.08,
    specs=[[{"type": "xy", "secondary_y": True}], [{"type": "xy"}], [{"type": "geo"}]],
    subplot_titles=("Monthly Climate & Health Trends (Global)", "Regional Analysis", "Global Temperature Distribution")
)

# Row 1: Time series (temperature, AQI, health risk)
fig.add_trace(go.Scatter(x=monthly_stats['Date'], y=monthly_stats['Temp'], name="Temp (°C)", line=dict(color='red')), row=1, col=1)
fig.add_trace(go.Scatter(x=monthly_stats['Date'], y=monthly_stats['AQI'], name="AQI", line=dict(color='green', dash='dash')), row=1, col=1)
fig.add_trace(go.Scatter(x=monthly_stats['Date'], y=monthly_stats['Health'], name="Health Risk %", line=dict(color='blue', dash='dot')), row=1, col=1, secondary_y=True)

# Row 2: Regional bar chart
fig.add_trace(go.Bar(x=regional_pd['Region'], y=regional_pd['Health'], name="Health %", marker_color='royalblue'), row=2, col=1)
fig.add_trace(go.Bar(x=regional_pd['Region'], y=regional_pd['AQI'], name="AQI", marker_color='mediumseagreen'), row=2, col=1)

# Row 3: Choropleth map with animation over years
years = sorted(map_pd['Year'].unique())
df_first = map_pd[map_pd['Year'] == years[0]]
fig.add_trace(go.Choropleth(
    locations=df_first["Country"], z=df_first["Temp"], locationmode='country names',
    colorscale="YlOrRd", name="Temperature",
    zmin=map_pd['Temp'].min(), zmax=map_pd['Temp'].max(),
    customdata=np.stack((df_first['AQI'], df_first['Health']), axis=-1),
    hovertemplate="<b>%{location}</b><br>Temp: %{z:.1f}°C<br>AQI: %{customdata[0]:.1f}<br>Health Risk: %{customdata[1]:.1f}%<extra></extra>"
), row=3, col=1)

# Animation frames
frames = [go.Frame(data=[go.Choropleth(locations=map_pd[map_pd['Year'] == y]["Country"], z=map_pd[map_pd['Year'] == y]["Temp"])], name=str(y), traces=[len(fig.data)-1]) for y in years]
fig.frames = frames

fig.update_layout(
    height=1200, template="plotly_white",
    title_text=f"BIG DATA PLATFORM | {row_count:,} Rows | JSON + CSV",
    sliders=[{"active": 0, "currentvalue": {"prefix": "Year: "}, "steps": [{"method": "animate", "args": [[str(y)], {"frame": {"duration": 300, "redraw": True}}], "label": str(y)} for y in years]}]
)
fig.write_html("Poonam_Kadam_Big_Data_Dashboard.html")
fig.show()

# ---- Country-wise Dashboard (interactive dropdown) ----
print("[Serving] Building country-wise dashboard...")
country_monthly = final_df.groupBy("Country", "Year", "Month").agg(
    spark_avg("Mean_Temp_C").alias("Temp"), spark_avg("AQI").alias("AQI"),
    spark_avg("Health_Risk").alias("Health"), spark_avg("CO2_Emissions").alias("CO2")
).orderBy("Year", "Month").toPandas()
country_monthly['Date'] = country_monthly['Year'].astype(str) + "-" + country_monthly['Month'].astype(str).str.zfill(2)
countries = sorted(country_monthly['Country'].unique())
df_default = country_monthly[country_monthly['Country'] == countries[0]]

fig2 = go.Figure()
fig2.add_trace(go.Scatter(x=df_default['Date'], y=df_default['Temp'], name="Temp (°C)", line=dict(color='red')))
fig2.add_trace(go.Scatter(x=df_default['Date'], y=df_default['AQI'], name="AQI", line=dict(color='green')))
fig2.add_trace(go.Scatter(x=df_default['Date'], y=df_default['Health'], name="Health Risk", line=dict(color='blue')))
fig2.add_trace(go.Scatter(x=df_default['Date'], y=df_default['CO2'], name="CO2 Emissions", line=dict(color='black', dash='dot')))

buttons = [dict(label=c, method="update", args=[{"x": [country_monthly[country_monthly['Country'] == c]['Date']]*4,
                                                  "y": [country_monthly[country_monthly['Country'] == c][col] for col in ['Temp', 'AQI', 'Health', 'CO2']]}]) for c in countries]

fig2.update_layout(updatemenus=[dict(buttons=buttons, direction="down", showactive=True, x=0.1, y=1.15)],
                   xaxis=dict(rangeslider=dict(visible=True), type="date"),
                   title="Country-wise Climate & Health Trends", height=600, template="plotly_white")
fig2.write_html("Country_Wise_Dashboard.html")
fig2.show()

print("\n✅ All dashboards generated. Batch processing complete.")