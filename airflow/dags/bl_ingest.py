from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os

# === CONFIG ===
SQL_DIR = "/opt/airflow/sql/bl"

PG_CONN = {
    "dbname": "hocotatefreight",
    "user": "shacho",
    "password": "20011026pikpikcarrots",
    "host": "postgres",
    "port": 5432,
}

default_args = {
    "owner": "hocotate-freight",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# === HELPERS ===
def run_sql_file(filename: str):
    """Execute an SQL file from the BL sql directory."""
    path = os.path.join(SQL_DIR, filename)
    with open(path, "r") as f:
        sql = f.read()

    print(f"⚙️ Running {filename}...")
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()
    print(f"✅ {filename} executed successfully.")


def mission_recently_updated():
    """Check if any mission was updated in the last 30 minutes."""
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) 
        FROM params.missions
        WHERE updated_at > now() - INTERVAL '30 minutes';
    """)
    result = cur.fetchone()
    conn.close()

    if result and result[0] > 0:
        print("✅ Recent mission updates detected (within 30 minutes). Proceeding with BL refresh.")
    else:
        raise ValueError("❌ No recent mission updates detected — skipping BL refresh.")


# === DAG ===
with DAG(
    dag_id="ll_to_bl_transform",
    description="Build BL aggregates from LL and params schemas",
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    check_recent_missions = PythonOperator(
        task_id="check_recent_mission_updates",
        python_callable=mission_recently_updated,
    )

    mission_summary = PythonOperator(
        task_id="build_mission_summary",
        python_callable=run_sql_file,
        op_args=["mission_summary.sql"],
    )

    cargo_summary = PythonOperator(
        task_id="build_cargo_summary",
        python_callable=run_sql_file,
        op_args=["cargo_summary.sql"],
    )

    manifest_summary = PythonOperator(
        task_id="build_manifest_summary",
        python_callable=run_sql_file,
        op_args=["manifest_summary.sql"],
    )

    planet_summary = PythonOperator(
        task_id="build_planet_summary",
        python_callable=run_sql_file,
        op_args=["planet_summary.sql"],
    )

    # --- Dependencies ---
    check_recent_missions >> mission_summary
    mission_summary >> [cargo_summary, manifest_summary]
    [cargo_summary, manifest_summary] >> planet_summary