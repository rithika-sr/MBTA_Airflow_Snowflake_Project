# ============================================
# DAG: MBTA to Snowflake Data Pipeline (Final)
# ============================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import snowflake.connector
import numpy as np

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

    # Handle NaN values
    df.replace({np.nan: None}, inplace=True)

    df.to_csv("/tmp/mbta_data.csv", index=False)
    print(f"✅ {len(df)} records fetched and saved to /tmp/mbta_data.csv")

# =====================================================
# Function: Load Data into Snowflake (with Safe Handling)
# =====================================================
def load_to_snowflake():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user="RITHIKA0311",
        password="Charliembta@12345",
        account="vrc94697.us-east-1",  
        warehouse="COMPUTE_WH",
        database="SNOWFLAKE_LEARNING_DB",
        schema="PUBLIC"
    )
    cur = conn.cursor()

    # Read the data from local file
    df = pd.read_csv("/tmp/mbta_data.csv")

    # Replace NaN / invalids with None
    df = df.where(pd.notnull(df), None)

    # Create table if not exists
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

    # Define helper to safely insert nulls
    def safe_val(v):
        if v is None or str(v).lower() in ['nan', 'none', 'null']:
            return None
        return v

    # Insert row by row
    insert_query = """
        INSERT INTO MBTA_LIVE_PREDICTIONS 
        (ROUTE, DIRECTION, STATUS, ARRIVAL_TIME, DEPARTURE_TIME, STOP_SEQ, LOAD_TIMESTAMP)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

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
    description="Fetch MBTA live data every 10 minutes and load into Snowflake",
    schedule_interval="*/10 * * * *",  # every 10 minutes
    start_date=datetime(2025, 10, 7),
    catchup=False,
    tags=["mbta", "snowflake", "api"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_mbta_data",
        python_callable=fetch_mbta_data
    )

    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake
    )

    fetch_task >> load_task  # task dependency
