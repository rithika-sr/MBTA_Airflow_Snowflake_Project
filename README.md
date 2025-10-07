\# 🚆 MBTA Airflow–Snowflake Data Pipeline



\### \*Automating Real-Time MBTA Transit Data with Airflow, Docker \& Snowflake\*



!\[Airflow + Snowflake](https://github.com/github/explore/raw/main/topics/airflow/airflow.png)



---



\## 📖 Project Overview



This project demonstrates an \*\*end-to-end automated data pipeline\*\* that fetches \*\*real-time MBTA (Massachusetts Bay Transportation Authority)\*\* predictions using the official API and loads them into a \*\*Snowflake data warehouse\*\* every 10 minutes using \*\*Apache Airflow\*\* orchestrated inside Docker.



It’s designed as a \*\*resume-ready data engineering project\*\* — showcasing ETL automation, API ingestion, cloud data warehousing, and containerized orchestration.



---



\## 🧩 Tech Stack



| Category        | Tools \& Technologies              |

| --------------- | --------------------------------- |

| Orchestration   | 🪶 Apache Airflow (Dockerized)    |

| Cloud Warehouse | ❄️ Snowflake                      |

| Programming     | 🐍 Python 3 (Pandas, Requests)    |

| Infrastructure  | 🐳 Docker Desktop                 |

| Scheduling      | ⏰ Cron (Airflow DAG every 10 min) |

| Data Source     | 🚇 MBTA Live Predictions API      |



---



\## ⚙️ Architecture



```mermaid

flowchart LR

&nbsp;   A\[🚇 MBTA API] --> B\[🐍 Python Extraction]

&nbsp;   B --> C\[🧹 Transform (Clean \& Handle NaN)]

&nbsp;   C --> D\[❄️ Load into Snowflake]

&nbsp;   D --> E\[📊 Query / Dashboard in Snowflake]

&nbsp;   F\[⏰ Airflow Scheduler] --> B

```



---



\## 📂 Repository Structure



```

MBTA\_Airflow\_Project/

│

├── dags/

│   └── mbta\_to\_snowflake\_dag.py      # Main Airflow DAG

│

├── docker-compose.yaml               # Airflow Docker environment

├── .gitignore                        # Ignore cache/logs/credentials

└── README.md                         # Project documentation

```



---



\## 🚀 How It Works



1\. \*\*Fetch MBTA Data\*\*



&nbsp;  \* The DAG calls the MBTA API endpoint `/predictions`

&nbsp;  \* Extracts route, direction, status, arrival/departure times

&nbsp;  \* Saves results locally → `/tmp/mbta\_data.csv`



2\. \*\*Load into Snowflake\*\*



&nbsp;  \* Reads the CSV into Pandas

&nbsp;  \* Creates table `MBTA\_LIVE\_PREDICTIONS` (if not exists)

&nbsp;  \* Inserts records via Snowflake Connector

&nbsp;  \* Handles `NaN` → `NULL` conversion safely



3\. \*\*Schedule via Airflow\*\*



&nbsp;  \* DAG runs every 10 minutes (`\*/10 \* \* \* \*`)

&nbsp;  \* Tasks:



&nbsp;    \* 🟩 `fetch\_mbta\_data` → 🟩 `load\_to\_snowflake`



4\. \*\*Monitor in Airflow UI\*\*



&nbsp;  \* Web UI → \[http://localhost:8080](http://localhost:8080)

&nbsp;  \* View DAG status (✅ success / ❌ failure / retry)



---



\## 🧠 Snowflake Table Schema



| Column         | Type      | Description              |

| -------------- | --------- | ------------------------ |

| ROUTE          | STRING    | MBTA line (e.g. Green-B) |

| DIRECTION      | INT       | Direction ID             |

| STATUS         | STRING    | Train status             |

| ARRIVAL\_TIME   | STRING    | Arrival timestamp        |

| DEPARTURE\_TIME | STRING    | Departure timestamp      |

| STOP\_SEQ       | INT       | Stop sequence            |

| LOAD\_TIMESTAMP | TIMESTAMP | Time of pipeline load    |



---



\## 🛠️ Setup \& Run



\### 1️⃣ Clone the Repository



```bash

git clone https://github.com/<your-username>/MBTA\_Airflow\_Project.git

cd MBTA\_Airflow\_Project

```



\### 2️⃣ Start Airflow with Docker



```bash

docker compose up -d

```



Wait until containers (`webserver`, `scheduler`, `worker`, `postgres`, `redis`) are running.



\### 3️⃣ Open Airflow UI



\[http://localhost:8080](http://localhost:8080)

Enable the DAG → `mbta\_to\_snowflake\_dag`



\### 4️⃣ Verify in Snowflake



```sql

SELECT \* 

FROM SNOWFLAKE\_LEARNING\_DB.PUBLIC.MBTA\_LIVE\_PREDICTIONS

ORDER BY LOAD\_TIMESTAMP DESC

LIMIT 20;

```



---



\## 🧾 Example Run Output



\*\*Airflow Logs\*\*



```

✅ 10 MBTA records fetched and saved to /tmp/mbta\_data.csv  

✅ Data inserted into Snowflake successfully!  

```



\*\*Snowflake Query Result\*\*



| ROUTE   | STATUS   | LOAD\_TIMESTAMP          |

| ------- | -------- | ----------------------- |

| Green-B | Arriving | 2025-10-07 18:00:00 UTC |

| Green-B | Departed | 2025-10-07 17:50:00 UTC |



---



\## 🔒 Security \& Best Practices



\* ✅ Store credentials securely in Airflow Connections (not in code)

\* ✅ Use `.gitignore` to exclude logs, .env files, and CSVs

\* ✅ Parameterize account details for portability





---



\## 🌟 Key Highlights



\* 🧠 Real-time data integration pipeline

\* ⚙️ Automated Airflow scheduling inside Docker

\* ❄️ Snowflake data warehouse integration

\* 🧹 Robust error handling \& NaN management

\* 🧾 Production-style architecture for portfolio







