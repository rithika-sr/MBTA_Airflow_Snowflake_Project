# MBTA Airflow – Snowflake Data Pipeline

## Overview
This project builds an **end-to-end data pipeline** using **Apache Airflow**, **Docker**, and **Snowflake** to automate the extraction and loading of live public transit data from the **MBTA (Massachusetts Bay Transportation Authority) API**.  
The pipeline fetches real-time predictions for the Green-B line and loads them into Snowflake for downstream analytics, dashboarding, or alerting.

---

## Project Architecture
**Data Flow:**
### MBTA API → Airflow DAG → Extract Task → Transform (CSV) → Load Task → Snowflake Table

**Tools Used:**
- **Apache Airflow** – Workflow orchestration and scheduling  
- **Docker Compose** – Containerized Airflow setup  
- **Snowflake** – Cloud data warehouse for scalable analytics  
- **Python** – Data extraction, cleaning, and integration logic  
- **Pandas** – Data transformation and CSV handling  
- **Requests** – REST API communication with MBTA

---

## DAG Details
**DAG ID:** `mbta_to_snowflake_dag`  
**Schedule Interval:** Every 10 minutes (`*/10 * * * *`)  
**Owner:** `rithika`  
**Description:** Fetch MBTA live data every 10 minutes and load it into Snowflake.

**Tasks:**
1. **fetch_mbta_data**  
   - Calls MBTA’s real-time predictions API  
   - Extracts route, direction, stop sequence, arrival/departure times, and status  
   - Saves results to `/tmp/mbta_data.csv`

2. **load_to_snowflake**  
   - Connects to Snowflake using the Snowflake Python Connector  
   - Loads data from `/tmp/mbta_data.csv` into the target table  
   - Commits the transaction and closes the connection

---

## Project Setup:

### 1. Prerequisites
- Docker and Docker Compose installed  
- Snowflake account (with database, schema, and warehouse)  
- MBTA API (no authentication required)

### 2. Clone the Repository

### 3. Configure Environment
-Update credentials directly in the DAG (or via environment variables).

### 4. Start Airflow with Docker

### 5. Trigger the DAG
- Enable and trigger the mbta_to_snowflake_dag in the Airflow UI.
- You’ll see two tasks: fetch_mbta_data → load_to_snowflake.
  <img width="2829" height="1509" alt="image" src="https://github.com/user-attachments/assets/c2855b20-1ccd-461e-bcb3-7cf5466808cd" />


## Snowflake Table Schema

**Table: MBTA_LIVE_PREDICTIONS**

| Column Name    | Data Type | Description                              |
| -------------- | --------- | ---------------------------------------- |
| ROUTE          | STRING    | MBTA line name (e.g., Green-B)           |
| DIRECTION      | INT       | Direction ID (0 = outbound, 1 = inbound) |
| STATUS         | STRING    | Train status                             |
| ARRIVAL_TIME   | TIMESTAMP | Expected arrival time                    |
| DEPARTURE_TIME | TIMESTAMP | Expected departure time                  |
| STOP_SEQ       | INT       | Stop sequence number                     |

<img width="2865" height="1492" alt="image" src="https://github.com/user-attachments/assets/e8af8026-4cf0-40cd-9daf-cfc48bf1de87" />


### Key Learnings

* Built a containerized ETL pipeline using Docker and Airflow

* Automated real-time ingestion from a public REST API

* Integrated Airflow with Snowflake via Python connector

* Implemented retry logic and task dependencies

* Developed practical skills in workflow orchestration and data warehousing
