
# 🚂 MBTA Airflow – Snowflake Data Pipeline

## Overview

This project builds a **complete end-to-end data pipeline** using **Apache Airflow**, **Docker**, **Snowflake**, and **Streamlit** to automate the extraction, validation, and visualization of **real-time public transit data** from the **MBTA (Massachusetts Bay Transportation Authority) API**.

The pipeline continuously fetches **live Green-B line predictions**, performs **data quality validation**, and loads the results into **Snowflake** for real-time analytics, dashboarding, and monitoring.

<p align="center">
  <img src="https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExaHdzZ2Q1eW82a3NyYmliMDh5NG1yc2U3OGVpM2l6cnNxbWdvbmRtZiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/BhcHf3qYMg8Zq/giphy.gif" alt="MBTA Green Line" width="600"/>
</p>

---

## ⚙️ Project Architecture

### Data Flow

#### **MBTA API → Airflow DAG → Data Validation Layer → Load to Snowflake → Streamlit Dashboard**

### Tools & Technologies

| Component           | Purpose                                                       |
| ------------------- | ------------------------------------------------------------- |
| **Apache Airflow**  | Orchestrates the pipeline (scheduling, dependencies, retries) |
| **Docker Compose**  | Containerizes Airflow for reproducibility                     |
| **Snowflake**       | Cloud data warehouse for structured storage                   |
| **Python + Pandas** | Data extraction, cleaning, and transformation                 |
| **Streamlit**       | Interactive dashboard for visualization                       |
| **Requests**        | API communication with MBTA endpoint                          |

---

## 🚦 DAG Details

<img width="2874" height="1507" alt="image" src="https://github.com/user-attachments/assets/0de786f3-337d-4a21-88c0-cd95c4649684" />

**DAG ID:** `mbta_to_snowflake_dag`
**Schedule Interval:** Every 10 minutes (`*/10 * * * *`)
**Description:** Fetches, validates, and loads live MBTA Green-B data into Snowflake.

### Tasks Overview

1. **`fetch_mbta_data`**

   * Connects to MBTA’s Predictions API
   * Extracts fields like route, direction, stop sequence, status, and timestamps
   * Saves as a local CSV file (`/tmp/mbta_data.csv`)

2. **`validate_data`** 🧩 *(Newly Added Layer)*

   * Checks schema consistency, nulls, and timestamp formats
   * Verifies directional values (must be `0` or `1`)
   * Logs any anomalies to `/tmp/mbta_pipeline_log.json`
   * Prevents corrupted data from being loaded into Snowflake

3. **`load_to_snowflake`**

   * Creates the target table if it doesn’t exist
   * Inserts validated rows safely into Snowflake
   * Commits and closes connection

---

## 🧱 Data Quality & Validation Layer

A new **validation layer** was introduced to improve reliability and trust in the pipeline.
It ensures only high-quality data flows into Snowflake.

### Key Checks Performed:

* ✅ File existence and structure validation
* ✅ Required column presence
* ✅ Null/empty route or timestamp detection
* ✅ Valid direction (0 = outbound, 1 = inbound)
* ✅ Timestamp format correctness (`YYYY-MM-DD HH:MM:SS`)

If validation fails, the DAG automatically halts the pipeline and logs the issue.

> 💡 *This makes your Airflow workflow production-grade — resilient against upstream data issues.*


---

## ❄️ Snowflake Table Schema

**Table:** `MBTA_LIVE_PREDICTIONS`

| Column Name    | Data Type | Description                              |
| -------------- | --------- | ---------------------------------------- |
| ROUTE          | STRING    | MBTA line name (e.g., Green-B)           |
| DIRECTION      | INT       | Direction ID (0 = Outbound, 1 = Inbound) |
| STATUS         | STRING    | Current train status                     |
| ARRIVAL_TIME   | TIMESTAMP | Expected arrival time                    |
| DEPARTURE_TIME | TIMESTAMP | Expected departure time                  |
| STOP_SEQ       | INT       | Stop sequence number                     |
| LOAD_TIMESTAMP | TIMESTAMP | When data was fetched by Airflow         |

<img width="2879" height="1517" alt="image" src="https://github.com/user-attachments/assets/a29ec237-58fa-4e35-8816-cfd8f6a86ce0" />

---

## 📊 Live Visualization Dashboard (Streamlit)

To bring the data to life, a **Streamlit dashboard** was built on top of Snowflake.
It displays near real-time MBTA metrics such as train direction breakdowns, arrival patterns, and data freshness — auto-updating every 5 minutes.

### ✨ Key Features

* 🔄 **Auto-refresh every 5 minutes** to keep data up to date
* 🧩 **System Overview** — total records, last load timestamp, and freshness indicator
* 📈 **Direction Comparison Chart** — shows inbound vs outbound train volume
* ⏱️ **Arrival Trend** — timeline of arrivals across stop sequences
* 📋 **Data Table** — collapsible view of the latest records
* 🧠 **Polished Footer** — branded project credit with timestamp for screenshots and portfolio use

* Streamlit System Overview section
<img width="2872" height="1431" alt="image" src="https://github.com/user-attachments/assets/3d8f93e1-9dd3-45e9-bf20-2507bbdeb170" />

* Direction Comparison bar chart
<img width="2808" height="1109" alt="image" src="https://github.com/user-attachments/assets/43111976-0702-4b98-94ea-f90573d295e0" />

* Arrival Trend line chart
<img width="2748" height="1111" alt="image" src="https://github.com/user-attachments/assets/dca0f393-a9d1-4c34-8b37-fd54840e44c6" />

* Latest Records table

<img width="2686" height="1072" alt="image" src="https://github.com/user-attachments/assets/f37847a0-d366-415f-842f-1139f4139d90" />


---

## ⚡ Project Setup

### 1️⃣ Prerequisites

* Install Docker, Docker Compose, and Python 3.9+
* Have a Snowflake account (with access to warehouse and schema)
* Clone the repository

  ```bash
  git clone https://github.com/rithika-sr/MBTA_Airflow_Snowflake_Project.git
  cd MBTA_Airflow_Snowflake_Project
  ```

### 2️⃣ Start Airflow via Docker

```bash
docker compose up -d
```

### 3️⃣ Access Airflow UI

Visit [http://localhost:8080](http://localhost:8080)
Enable and trigger the DAG: `mbta_to_snowflake_dag`

### 4️⃣ Start the Streamlit App

```bash
cd MBTA_Streamlit_App
streamlit run app.py
```

Visit [http://localhost:8501](http://localhost:8501)

---

## 💡 Key Learnings

* Designed a **containerized ETL workflow** using Airflow and Docker
* Implemented a **data validation layer** to ensure data integrity
* Built a **Snowflake integration** with Python for automated loading
* Created a **real-time dashboard** with Streamlit to monitor data health and trends
* Improved understanding of **workflow orchestration**, **data freshness tracking**, and **observability**


---

## 🧠 Future Enhancements

* Incorporate **Airflow SLAs and alerts** for missed data updates
* Add **historical trend aggregation** for performance insights
* Deploy dashboard to Streamlit Cloud or EC2 for public sharing


