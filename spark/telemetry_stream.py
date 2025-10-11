from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from minio import Minio
import threading

# === CONFIG ===
KAFKA_BROKER = "kafka:9094"
TOPIC = "telemetry.raw"
BUCKET = "telemetry-lake"
PARQUET_PATH = f"s3a://{BUCKET}/telemetry.parquet"
CHECKPOINT_PATH = f"s3a://{BUCKET}/_checkpoints/telemetry"
PUSHGATEWAY = "pushgateway:9091"

# === MINIO BUCKET CREATION ===
def ensure_bucket_exists():
    client = Minio(
        "minio:9000",
        access_key="shacho",
        secret_key="20011026pikpikcarrots",
        secure=False
    )
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        print(f"Created bucket '{BUCKET}' in MinIO.")
    else:
        print(f"Bucket '{BUCKET}' already exists.")

ensure_bucket_exists()

# === PROMETHEUS HELPERS ===
def push_metric(row):
    """Push all telemetry fields to Prometheus (adds repair/failure gauges)."""
    try:
        registry = CollectorRegistry()

        gauges = {
            'ship_fuel_percent': ('Current fuel percentage', row.fuel_prc),
            'ship_coord_x': ('Ship X coordinate', row.coord_x),
            'ship_coord_y': ('Ship Y coordinate', row.coord_y),
            'ship_coord_z': ('Ship Z coordinate', row.coord_z),
            'ship_motor_temp_c': ('Motor temperature (C)', row.motor_temp_c),
            'ship_engine_pressure_kpa': ('Engine pressure (kPa)', row.engine_pressure_kpa),
            'ship_power_core_temp_c': ('Power core temperature (C)', row.power_core_temp_c),
            'ship_navigation_signal_db': ('Navigation signal (dB)', row.navigation_signal_db),
            'ship_distance_km': ('Distance from Hocotate (km)', row.distance_from_hocotate_km),
        }

        # Add discrete state metrics
        if hasattr(row, "status"):
            gauges["ship_repair_flag"] = (
                "1 if ship under repair, else 0",
                1.0 if str(row.status).upper() == "REPAIR" else 0.0
            )

        if hasattr(row, "mission_status"):
            gauges["ship_mission_success"] = (
                "1 if mission success, 0 if failure or running",
                1.0 if str(row.mission_status).upper() == "SUCCESS" else 0.0
            )
            gauges["ship_mission_failure"] = (
                "1 if mission failed, 0 otherwise",
                1.0 if str(row.mission_status).upper() == "FAILURE" else 0.0
            )

        for metric_name, (description, value) in gauges.items():
            g = Gauge(metric_name, description, ['ship'], registry=registry)
            g.labels(ship=row.ship_id).set(float(value))

        push_to_gateway(PUSHGATEWAY, job="ship_telemetry", registry=registry)
    except Exception as e:
        print(f"[WARN] Prometheus push failed: {e}")

def push_metrics_async(rows):
    def _push_all():
        for row in rows:
            push_metric(row)
    thread = threading.Thread(target=_push_all, daemon=True)
    thread.start()

# === SPARK SESSION ===
spark = (
    SparkSession.builder
        .appName("TelemetryStream")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "shacho")
        .config("spark.hadoop.fs.s3a.secret.key", "20011026pikpikcarrots")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# === TELEMETRY SCHEMA ===
telemetry_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("mission_id", StringType()),
    StructField("ship_id", StringType()),
    StructField("coord_x", DoubleType()),
    StructField("coord_y", DoubleType()),
    StructField("coord_z", DoubleType()),
    StructField("fuel_prc", DoubleType()),
    StructField("motor_temp_c", DoubleType()),
    StructField("engine_pressure_kpa", DoubleType()),
    StructField("power_core_temp_c", DoubleType()),
    StructField("navigation_signal_db", DoubleType()),
    StructField("distance_from_hocotate_km", DoubleType()),
    # new status & mission_status
    StructField("status", StringType()),
    StructField("mission_status", StringType()),
])

# === PER-BATCH HANDLER ===
def handle_batch(df, batch_id):
    if df.isEmpty():
        return

    parsed = (
        df.selectExpr("CAST(value AS STRING) as json")
          .select(from_json(col("json"), telemetry_schema).alias("data"))
          .select("data.*")
    )

    rows = parsed.collect()
    push_metrics_async(rows)

    # Append batch to Parquet (partitioned by mission_id)
    parsed.write.format("parquet") \
        .mode("append") \
        .partitionBy("mission_id") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .save()

# === STREAM QUERY ===
(
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BROKER)
         .option("subscribe", TOPIC)
         .option("startingOffsets", "latest")
         .load()
         .writeStream
         .foreachBatch(handle_batch)
         .trigger(processingTime="1 second")
         .start()
         .awaitTermination()
)