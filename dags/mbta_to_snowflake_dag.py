# ============================================
# DAG: MBTA to Snowflake Data Pipeline (Validation + Logging)
# ============================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import requests
import pandas as pd
import snowflake.connector
import numpy as np
from pathlib import Path
import json

# =====================================================
# Utility: Log pipeline metrics to a local JSON file
# =====================================================
def log_pipeline_metrics(stage, status, record_count=None, message=None):
    log_entry = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "stage": stage,
        "status": status,
        "record_count": record_count,
        "message": message,
    }

    log_path = "/tmp/mbta_pipeline_log.json"

    try:
        with open(log_path, "r") as f:
            logs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logs = []

    logs.append(log_entry)

    with open(log_path, "w") as f:
        json.dump(logs, f, indent=4)

    print(f"📘 Logged pipeline event: {log_entry}")

# =====================================================
# Function: Fetch MBTA Live Predictions API Data
# =====================================================
def fetch_mbta_data():
    url = "https://api-v3.mbta.com/predictions"
    params = {
        "filter[route]": "Green-B",
        "sort": "departure_time",
        "page[limit]": 10,
        "include": "stop,route"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        rows = []
        for item in data.get("data", []):
            attr = item.get("attributes", {})
            rows.append({
                "route": "Green-B",
                "direction": attr.get("direction_id"),
                "status": attr.get("status"),
                "arrival_time": attr.get("arrival_time"),
                "departure_time": attr.get("departure_time"),
                "stop_seq": attr.get("stop_sequence"),
                "load_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })

        df = pd.DataFrame(rows)
        df.replace({np.nan: None}, inplace=True)
        df.to_csv("/tmp/mbta_data.csv", index=False)

        print(f"✅ {len(df)} records fetched and saved to /tmp/mbta_data.csv")
        log_pipeline_metrics("fetch", "success", record_count=len(df), message="Fetched MBTA predictions successfully.")

    except Exception as e:
        log_pipeline_metrics("fetch", "failed", message=str(e))
        raise

# =====================================================
# Function: Validate Data Quality
# =====================================================
def validate_data():
    csv_path = Path("/tmp/mbta_data.csv")

    if not csv_path.exists():
        log_pipeline_metrics("validate", "failed", message="CSV file not found.")
        raise FileNotFoundError("❌ Expected /tmp/mbta_data.csv from fetch_mbta_data, but file not found.")

    df = pd.read_csv(csv_path)

    try:
        # 1️⃣ Basic structure checks
        if df.empty:
            log_pipeline_metrics("validate", "failed", message="DataFrame is empty.")
            raise ValueError("❌ Data validation failed: DataFrame is empty.")

        required_cols = ["route", "direction", "status", "arrival_time", "departure_time", "stop_seq", "load_timestamp"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            log_pipeline_metrics("validate", "failed", message=f"Missing columns: {missing}")
            raise ValueError(f"❌ Data validation failed: Missing columns: {missing}")

        # 2️⃣ Null checks
        if df["route"].isna().any():
            log_pipeline_metrics("validate", "failed", message="'route' contains nulls.")
            raise ValueError("❌ Data validation failed: 'route' contains nulls.")
        if df["departure_time"].notna().sum() == 0 and df["arrival_time"].notna().sum() == 0:
            log_pipeline_metrics("validate", "failed", message="No valid timestamps found.")
            raise ValueError("❌ Data validation failed: No valid timestamps found.")

        # 3️⃣ Sanity checks for direction values
        valid_dirs = {0, 1}
        if not df["direction"].dropna().isin(valid_dirs).all():
            log_pipeline_metrics("validate", "failed", message="'direction' contains invalid values.")
            raise ValueError("❌ Data validation failed: 'direction' contains invalid values (not 0 or 1).")

        # 4️⃣ Timestamp format validation
        for col in ["arrival_time", "departure_time"]:
            if df[col].notna().any():
                pd.to_datetime(df[col].dropna(), errors="raise")

        print(f"✅ Data validation passed: {len(df)} rows, columns verified, timestamps valid.")
        log_pipeline_metrics("validate", "success", record_count=len(df), message="Data validation passed successfully.")

    except Exception as e:
        log_pipeline_metrics("validate", "failed", message=str(e))
        raise

# =====================================================
# Function: Load Data into Snowflake
# =====================================================
def load_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user="RITHIKA0311",
            password="Charliembta@12345",
            account="vrc94697.us-east-1",
            warehouse="COMPUTE_WH",
            database="SNOWFLAKE_LEARNING_DB",
            schema="PUBLIC"
        )
        cur = conn.cursor()

        df = pd.read_csv("/tmp/mbta_data.csv")
        df = df.where(pd.notnull(df), None)

        # Ensure target table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS MBTA_LIVE_PREDICTIONS (
                ROUTE STRING,
                DIRECTION INT,
                STATUS STRING,
                ARRIVAL_TIME STRING,
                DEPARTURE_TIME STRING,
                STOP_SEQ INT,
                LOAD_TIMESTAMP TIMESTAMP_NTZ
            )
        """)

        insert_query = """
            INSERT INTO MBTA_LIVE_PREDICTIONS 
            (ROUTE, DIRECTION, STATUS, ARRIVAL_TIME, DEPARTURE_TIME, STOP_SEQ, LOAD_TIMESTAMP)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        def safe_val(v):
            if v is None or str(v).lower() in ["nan", "none", "null"]:
                return None
            return v

        for _, row in df.iterrows():
            cur.execute(insert_query, (
                safe_val(row["route"]),
                int(row["direction"]) if pd.notna(row["direction"]) else None,
                safe_val(row["status"]),
                safe_val(row["arrival_time"]),
                safe_val(row["departure_time"]),
                int(row["stop_seq"]) if pd.notna(row["stop_seq"]) else None,
                safe_val(row["load_timestamp"])
            ))

        conn.commit()
        cur.close()
        conn.close()

        print("✅ Data inserted into Snowflake successfully!")
        log_pipeline_metrics("load", "success", record_count=len(df), message="Data loaded into Snowflake successfully.")

    except Exception as e:
        log_pipeline_metrics("load", "failed", message=str(e))
        raise

# =====================================================
# DAG Definition
# =====================================================
default_args = {
    "owner": "rithika",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="mbta_to_snowflake_dag",
    default_args=default_args,
    description="Fetch MBTA live data every 10 minutes, validate, and load into Snowflake",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2025, 10, 7),
    catchup=False,
    tags=["mbta", "snowflake", "api", "validation", "logging"],
) as dag:

    # Task 1: Extract MBTA data
    fetch_task = PythonOperator(
        task_id="fetch_mbta_data",
        python_callable=fetch_mbta_data
    )

    # Task 2: Validate the fetched data
    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    # Task 3: Load validated data into Snowflake
    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
        trigger_rule=TriggerRule.ALL_SUCCESS  # runs only if previous tasks succeed
    )

    # DAG flow: Fetch → Validate → Load
    fetch_task >> validate_task >> load_task
