# 🌍 Scalable Big Data Platform for Climate-Health Analytics
## 👩‍💻 Author : Poonam Vikram Kadam**
               MSc Computer Science
               Reykjavik University

##  Overview
This project designs and implements a scalable Big Data platform to analyze the relationship between climate change and public health. It integrates multiple datasets (climate, air quality, and health) and performs large-scale analytics using Apache Spark.
The system demonstrates how Big Data technologies can support data-driven decision-making for climate-health analysis.

##  Features
* Integration of heterogeneous datasets (JSON & CSV)
* Data cleaning and transformation using PySpark
* K-Means clustering (risk classification of countries)
* Correlation analysis (temperature vs health risk)
* Time-series trend analysis
* Interactive dashboards using Plotly
* Scalable architecture (Big Data ready)

##  Tech Stack
* **Python**
* **Apache Spark (PySpark)**
* Pandas, NumPy
* Plotly (Visualization)

*Designed for future integration with Apache Kafka, HDFS, and Cassandra*

##  Data Sources
* World Bank Climate Data
* WHO Health Data
* PM2.5 Air Quality Data
* CO2 Emissions Data

## 📂 Project Structure
```
├── main_script.py
├── main_climate_data.json
├── main_pm25_data.csv
├── co2_emissions.csv
├── dashboards/
│   ├── Global_Dashboard.html
│   └── Country_Wise_Dashboard.html
├── report.pdf
└── README.md
```

## ⚙️ How to Run
### 1. Install Dependencies
```bash
pip install pandas numpy pyspark plotly
```
### 2. Run the Script
```bash
Climate_pipeline.py
```
### 3. Output
* Interactive dashboards will be generated:
  * Global Dashboard
  * Country-wise Dashboard
##  Output & Results
* Processes large-scale datasets (millions of rows)
* Generates interactive visual dashboards
* Identifies climate-health patterns across countries
(See implementation details in report )

##  Architecture
**Batch Processing Pipeline:**
Data Sources → Ingestion → Spark Processing → Analytics → Visualization
 *Future Extension:*
* Apache Kafka (Streaming)
* Spark Streaming (Real-time analytics)
* Cassandra (Serving layer)

## Limitations

* Prototype uses local storage instead of HDFS
* Limited dataset coverage
* Real-time processing not fully implemented

## Future Work
* Real-time data processing with Kafka
* Cloud deployment (AWS/Azure/GCP)
* Additional datasets (GDP, FAOSTAT)
* Advanced ML models

##  Notes
This project demonstrates all **5 V’s of Big Data**:
* Volume
* Variety
* Velocity
* Veracity
* Value

*End of Project*
