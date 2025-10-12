from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pandas as pd
import psycopg2
import io
import json
import hashlib

# === CONFIG ===
MINIO_ENDPOINT = "minio:9000"
MINIO_USER = "shacho"
MINIO_PASS = "20011026pikpikcarrots"
BUCKET = "missions"

PG_CONN = {
    "dbname": "hocotatefreight",
    "user": "shacho",
    "password": "20011026pikpikcarrots",
    "host": "postgres",
    "port": 5432,
}

# === HELPERS ===
def list_manifests():
    """List manifest files in MinIO (flat scan)"""
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)
    objs = client.list_objects(BUCKET, recursive=True)
    manifests = [o.object_name for o in objs if o.object_name.endswith(".json")]
    return manifests


def file_checksum_bytes(data: bytes):
    return hashlib.sha256(data).hexdigest()


def load_manifest_into_db(manifest_name, **context):
    """Process one manifest: validate and insert its parquet into LL schema"""
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)

    # --- 1️⃣ Fetch manifest ---
    manifest_obj = client.get_object(BUCKET, manifest_name)
    manifest_json = json.loads(manifest_obj.read().decode("utf-8"))
    manifest_obj.close()

    mission_id = manifest_json["mission_id"]
    mission_date = manifest_json["date"]
    mission_key = manifest_json["files"]["mission"]
    checksum_mission = manifest_json["checksum"]["mission"]
    upload_time = manifest_json["upload_time"]

    # --- 2️⃣ Connect to Postgres ---
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    # Skip if already processed
    cur.execute("""
        SELECT 1 FROM LL.manifests
        WHERE mission_id=%s AND mission_date=%s;
    """, (mission_id, mission_date))
    if cur.fetchone():
        print(f"⏩ Manifest {manifest_name} already processed, skipping.")
        conn.close()
        return

    # --- 3️⃣ Fetch mission parquet ---
    mission_obj = client.get_object(BUCKET, mission_key)
    parquet_bytes = mission_obj.read()
    mission_obj.close()

    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    df["file_checksum"] = checksum_mission
    df["source_file"] = mission_key
    df["loaded_at"] = datetime.utcnow()

    # --- 4️⃣ Insert into LL.mission_reports ---
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert("""
        COPY LL.mission_reports (mission_id, item, value, timestamp, file_checksum, source_file, loaded_at)
        FROM STDIN WITH (FORMAT csv);
    """, buffer)

    # --- 5️⃣ Insert ship state (supports both flat + nested coords) ---
    ship_state = manifest_json.get("ship_state")
    if ship_state:
        coords = ship_state.get("coordinates") or {
            "x_km": ship_state.get("coord_x"),
            "y_km": ship_state.get("coord_y"),
            "z_km": ship_state.get("coord_z"),
            "planet": ship_state.get("planet", "UNKNOWN"),
        }

        cur.execute("""
            INSERT INTO LL.ship_state (
                mission_id, timestamp, fuel_prc, ship_condition,
                temperature_C, radiation_uSv, cargo_integrity_prc,
                coord_x, coord_y, coord_z, planet, file_checksum, source_file
            )
            VALUES (
                %(mission_id)s, now(), %(fuel)s, %(state)s, %(temp)s, %(rad)s,
                %(cargo)s, %(x)s, %(y)s, %(z)s, %(planet)s, %(checksum)s, %(src)s
            )
        """, {
            "mission_id": mission_id,
            "fuel": ship_state.get("fuel_prc", 0),
            "state": ship_state.get("ship_condition", "UNKNOWN"),
            "temp": ship_state.get("temperature_C", ship_state.get("motor_temp_c", 0.0)),
            "rad": ship_state.get("radiation_uSv", 0.0),
            "cargo": ship_state.get("cargo_integrity_prc", 100),
            "x": coords.get("x_km", 0),
            "y": coords.get("y_km", 0),
            "z": coords.get("z_km", 0),
            "planet": coords.get("planet", "UNKNOWN"),
            "checksum": checksum_mission,
            "src": mission_key,
        })

    # --- 6️⃣ Register manifest ---
    cur.execute("""
        INSERT INTO LL.manifests (
            mission_id, mission_date, manifest_path, mission_file,
            checksum_mission, checksum_manifest, upload_time, status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        mission_id, mission_date, manifest_name, mission_key,
        checksum_mission, file_checksum_bytes(json.dumps(manifest_json).encode()),
        upload_time, "INGESTED"
    ))

    conn.commit()
    conn.close()
    print(f"✅ Manifest {manifest_name} ingested into LL schema.")


# === DAG DEFINITION ===
with DAG(
    "minio_to_ll_ingest",
    description="Micro-batch ingestion from MinIO to Postgres LL schema",
    schedule_interval="*/15 * * * *",  # every 15 minutes
    start_date=datetime(2025, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "hocotate-freight", "retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:

    def discover_and_process(**context):
        manifests = list_manifests()
        for m in manifests:
            load_manifest_into_db(m)

    ingest_task = PythonOperator(
        task_id="scan_and_ingest_manifests",
        python_callable=discover_and_process,
        provide_context=True,
    )

    ingest_task