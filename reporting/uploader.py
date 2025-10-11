from minio import Minio
from minio.error import S3Error
import time
from pathlib import Path
import hashlib
import json
from datetime import datetime


def file_checksum(path: Path) -> str:
    """Compute SHA-256 checksum for validation."""
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def upload_log_event(client, cfg, source_bucket, file_path, checksum, success=True, mission_id=None):
    """Upload a JSON log describing each upload attempt (in the year +1000)."""
    logs_bucket = cfg["minio"]["buckets"]["logs"]

    # Ensure bucket exists
    if not client.bucket_exists(logs_bucket):
        client.make_bucket(logs_bucket)

    timestamp = datetime.utcnow().replace(year=datetime.utcnow().year + 1000)
    date_prefix = timestamp.strftime("%Y-%m-%d")
    time_tag = timestamp.strftime("%Y-%m-%dT%H-%M-%SZ")

    # If mission_id not provided, try to extract from filename
    if not mission_id:
        stem = file_path.stem
        parts = stem.split("_")
        # The mission ID is typically the second numeric part (after "mission"/"log"/etc)
        # Skip the first part (the type) and look for a numeric part that's NOT 8 digits (the date)
        numeric_parts = [p for p in parts[1:] if p.isdigit()]
        # The mission ID is the shorter numeric part, date is 8 digits (YYYYMMDD)
        mission_id = next((p for p in numeric_parts if len(p) < 8), "unknown")
    
    stem = file_path.stem
    log_type = stem.split("_")[0] if "_" in stem else "file"

    log_event = {
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mission_id": mission_id,
        "file_name": file_path.name,
        "source_bucket": source_bucket,
        "status": "SUCCESS" if success else "FAIL",
        "checksum": checksum,
    }

    log_name = f"log_{log_type}_{mission_id}_{time_tag}.json"
    tmp_dir = Path("data/logs")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / log_name
    tmp_path.write_text(json.dumps(log_event, indent=2))

    object_key = f"{date_prefix}/mission_{mission_id}/{log_name}"
    client.fput_object(logs_bucket, object_key, str(tmp_path))
    print(f"🧾 Uploaded log event → {logs_bucket}/{object_key}")


def upload_file(
    file_path: Path,
    bucket: str,
    cfg: dict,
    max_retries: int = 3,
    delay: int = 3,
    object_name: str | None = None,
    mission_id: str | None = None,
):
    """Upload file to MinIO (+1000 years timestamped logs)."""
    endpoint = cfg["minio"]["endpoint"].replace("http://", "").replace("https://", "")
    access_key = cfg["minio"]["access_key"]
    secret_key = cfg["minio"]["secret_key"]

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"🪣 Created missing bucket '{bucket}'")
    except S3Error as e:
        print(f"❌ Bucket check error: {e}")
        upload_log_event(client, cfg, bucket, file_path, "n/a", success=False, mission_id=mission_id)
        return False

    local_hash = file_checksum(file_path)
    file_key = object_name if object_name else file_path.name

    for attempt in range(1, max_retries + 1):
        try:
            client.fput_object(bucket, file_key, str(file_path))
            print(f"🚀 Uploaded {file_path.name} → bucket:{bucket}/{file_key}")

            stat = client.stat_object(bucket, file_key)
            remote_hash = stat.etag.replace('"', '')

            print(f"   • Local SHA256: {local_hash[:12]}…  Remote ETag: {remote_hash[:12]}…")

            upload_log_event(client, cfg, bucket, file_path, local_hash, success=True, mission_id=mission_id)
            return True

        except Exception as e:
            print(f"⚠️ Upload failed (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                print(f"❌ Giving up on {file_path.name} after {max_retries} failed attempts.")
                upload_log_event(client, cfg, bucket, file_path, local_hash, success=False, mission_id=mission_id)
                return False