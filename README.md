# 🌫️ India Air Quality & Weather Intelligence Pipeline

A production-grade, real-time data engineering pipeline that ingests air quality and weather data for all **32 Indian state capitals**, processes it through a **Medallion Architecture** on Azure Databricks, and serves analytics-ready data to Power BI dashboards.

---

## 📐 Architecture Overview

```
Open-Meteo APIs
      │
      ▼
┌─────────────┐
│   BRONZE    │  Raw ingestion — air quality + weather data per city
│  Delta Lake │  (append-only, immutable source of truth)
└──────┬──────┘
       │  PySpark Structured Streaming
       ▼
┌─────────────┐
│   SILVER    │  Cleaned, typed, joined & enriched data
│  Delta Lake │  (timestamp parsing, watermarking, stream-stream join)
└──────┬──────┘
       │  PySpark Batch + Window Functions
       ▼
┌─────────────┐
│    GOLD     │  Analytics-ready: reports, rolling stats, alerts, dimensions
│  Delta Lake │  (served to Power BI via Unity Catalog)
└─────────────┘
       │
       ▼
  Power BI Dashboard
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Compute | Azure Databricks |
| Processing | PySpark (Structured Streaming + Batch) |
| Table Format | Delta Lake |
| Catalog | Unity Catalog (bicatalog) |
| Visualization | Power BI |
| Data Source | Open-Meteo API (free, no key required) |
| Auth | Azure Service Principal (OAuth2) |

---

## 📂 Project Structure

```
india-air-quality-pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion.py        # Fetches AQI + weather from Open-Meteo API
│   └── 02_silver_gold_processing.py  # Streaming transforms, Gold aggregations
└── README.md
```

---

## 🔄 Pipeline Details

### Notebook 1 — Bronze Ingestion (`01_bronze_ingestion.py`)

- Calls the **Open-Meteo Air Quality API** for hourly AQI, PM2.5, PM10, CO, NO₂, SO₂, O₃
- Calls the **Open-Meteo Weather Archive API** for daily max/min temp, wind speed, precipitation
- Covers all **32 Indian state capitals** (28 states + 4 union territories)
- Writes raw data to Delta Lake bronze layer on ADLS Gen2

### Notebook 2 — Silver & Gold Processing (`02_silver_gold_processing.py`)

**Silver Layer (Streaming)**
- Parses timestamps, extracts date parts (year, month, day)
- Stream-stream join of air quality + weather streams with **10-minute watermarking**
- Enriches with:
  - `danger_level` — 5-tier classification (Safe → Hazardous) based on AQI
  - `risk_score` — weighted composite formula: `AQI(35%) + PM2.5(25%) + PM10(15%) + CO(15%) + NO₂(10%)`
  - `risk_level` — Low / Moderate / High / Very High / Critical
  - `geo_risk_zone` — Safe / Yellow / Orange / Red (for map visualizations)
  - `diseases` — health effects associated with AQI range
  - `recommendation` — public safety advisory

**Gold Layer (Batch + Streaming)**

| Table | Description |
|---|---|
| `environment_report` | Monthly aggregates (avg/max/min) per city |
| `environment_report_rolling_data` | 7-day rolling window stats per city |
| `environment_alert` | Filtered records where AQI > 300 or risk score > 60 |
| `fact_environment` | Fact table for dimensional model |
| `location_dim` | Dimension table: city, state, lat/lon, SHA-256 key |
| `date_dim` | Dimension table: date and timestamp keys |

---

## 📊 Key Features

- **Real-time streaming** with PySpark Structured Streaming and `trigger(availableNow=True)`
- **Stream-stream joins** with event-time watermarking to handle late data
- **Rolling window analytics** using `Window.rowsBetween(-6, 0)` for 7-day trends
- **Surrogate keys** generated with SHA-256 hash (`sha2 + concat_ws`) for dimension tables
- **Secrets management** via Databricks Secret Scopes (no hardcoded credentials)
- **Unity Catalog** integration for Power BI consumption

---

## ⚙️ Setup & Configuration

### Prerequisites
- Azure subscription with ADLS Gen2 storage account
- Azure Databricks workspace
- Service Principal with Storage Blob Data Contributor role

### ADLS Container Structure
Create three containers in your storage account:
```
bronze/
silver/
gold/
```

### Databricks Secret Scope
Store credentials in a Databricks secret scope named `environment_scope`:
```
clientid   → Service Principal Client ID
Appid      → App (Client) ID
tenantid   → Azure Tenant ID
```

### Running the Pipeline
1. Import both notebooks into your Databricks workspace
2. Update the storage account name in the ABFS paths
3. Run `01_bronze_ingestion.py` first to populate the bronze layer
4. Run `02_silver_gold_processing.py` to process through silver and gold

---

## 📈 Data Coverage

All 32 Indian state capitals including:
- 28 states: Andhra Pradesh to West Bengal
- 4 Union Territories: Delhi, J&K, Ladakh, Puducherry

---

## 🔌 Data Sources

- [Open-Meteo Air Quality API](https://open-meteo.com/en/docs/air-quality-api) — free, no API key required
- [Open-Meteo Weather Archive API](https://open-meteo.com/en/docs/historical-weather-api) — free, no API key required

---

## 📌 Notes

- This project was built as part of learning Azure Databricks + Delta Lake data engineering patterns
- The Power BI report connects to Unity Catalog gold tables via the Databricks connector
- All credentials are managed through Databricks Secrets — no keys are stored in code
